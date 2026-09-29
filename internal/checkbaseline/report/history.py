#!/usr/bin/env python3
"""Archive immutable local reports and rebuild a portable comparison index."""
import argparse
from contextlib import contextmanager
import fcntl
import math
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import render

ENV_KEYS = ('schema_mode','schema_cache','go', 'os', 'arch', 'cpu_model', 'cpus', 'gomaxprocs', 'classic', 'qp', 'backend_version', 'backend_settings', 'gogc', 'gomemlimit', 'godebug', 'build_settings')


@contextmanager
def file_lock(path, blocking=True):
    with Path(path).open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        try: yield
        finally: fcntl.flock(lock, fcntl.LOCK_UN)


def configuration_known(data):
    p=data['provenance']
    if p.get('backend')=='postgres':
        if not p.get('backend_version'):return False
        try:
            settings=json.loads(p.get('backend_settings',''))
            required=('shared_buffers','work_mem','effective_cache_size','fsync','autovacuum','jit','plan_cache_mode','enable_indexscan','enable_indexonlyscan','enable_bitmapscan','enable_seqscan','pool_read_max','pool_write_max','cache_policy','preparation')
            if not all(isinstance(settings.get(k),str) and settings[k] for k in required):return False
            deployment=json.loads(settings['container'])
            if not all(isinstance(deployment.get(k),str) and deployment[k] for k in ('image','architecture','dockerVersion','transport')):return False
            if not all(type(deployment.get(k)) in (int,float) and math.isfinite(deployment[k]) and deployment[k]>0 for k in ('cpus','memoryBytes','vmCPUs','vmMemoryBytes')):return False
        except (ValueError,TypeError,KeyError,AttributeError):return False
    return all(p.get(k) for k in ('go','os','arch','cpu_model','gomaxprocs','gogc','gomemlimit','build_settings')) and 'godebug' in p


def case_key(row):
    return hashlib.sha256(canonical(dict(dataset=row['dataset']['ID'],fingerprint=row['dataset']['Hash'],case=row['case'],profile=row['profile'])).encode()).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(path):
    with Path(path).open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def environment(data):
    p = data['provenance']
    return {**{k:p.get(k, '') for k in ENV_KEYS}, 'backend':p.get('backend', 'memdb')}


def comparison_key(data, row):
    if not configuration_known(data):return None
    value = dict(dataset=row['dataset']['ID'], fingerprint=row['dataset']['Hash'], case=row['case'],
                 profile=row['profile'], policy=data['policy'], environment=environment(data))
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def load(path):
    path = Path(path)
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt') as f:
        return json.load(f)


def merge_inputs(artifacts):
    result = None
    seen = set()
    sources = []
    for artifact in artifacts:
        if artifact.get('Samples', 0) < 10 or not artifact.get('Results'):
            raise ValueError('published runs require measured cases and at least ten samples')
        if any('ERROR:' in x for x in artifact.get('Omissions') or []):
            raise ValueError('cannot publish failed dataset setup')
        for row in artifact['Results']:
            if not row['Valid']:
                raise ValueError('cannot publish an invalid comparison as a completed run')
            if [e['Name'] for e in row['Engines']] != ['classic','qp']:
                raise ValueError('expected classic/QP engine pair')
            for engine in row['Engines']:
                for sample in engine.get('Samples') or []:
                    if not math.isfinite(sample['NSPerOp']) or sample['NSPerOp']<=0 or sample['Iterations']<1:
                        raise ValueError('invalid timing sample')
                    if any(not math.isfinite(sample[k]) or sample[k]<0 for k in ('BytesPerOp','AllocsPerOp')):
                        raise ValueError('invalid allocation sample')
                if len(engine.get('Samples') or []) != artifact['Samples']:
                    raise ValueError('partial timing samples')
                if len(engine.get('Work') or []) != artifact['Repetitions']:
                    raise ValueError('partial work observations')
                if any(w.get('LateEvents', 0) for w in engine['Work']):
                    raise ValueError('late work observed')
        data = render.compact(artifact)
        if data['provenance'].get('backend')=='postgres' and not configuration_known(data):
            raise ValueError('PostgreSQL publication requires complete runtime, server and deployment settings')
        for raw, row in zip(artifact['Results'], data['rows']):
            for original, engine in zip(raw['Engines'], row['engines']):
                engine['workFingerprint'] = hashlib.sha256(canonical(original['Work'][0]).encode()).hexdigest()
        if result is None:
            result = dict(data, rows=[], omissions=[])
        elif not configuration_known(data) or not configuration_known(result):
            raise ValueError('cannot merge shards with unknown machine/runtime configuration')
        for key in ('policy', 'samples', 'repetitions'):
            if data[key] != result[key]:
                raise ValueError('conflicting '+key+' across shards')
        if environment(data) != environment(result) or data['provenance'].get('commit') != result['provenance'].get('commit'):
            raise ValueError('conflicting source or environment across shards')
        if data['provenance'].get('dirty'):
            raise ValueError('publish measurements from a clean committed source tree')
        sources.append(dict(created=data['created'], invocation=data['provenance'].get('invocation', '')))
        for row in data['rows']:
            key = (row['dataset']['ID'], row['case']['ID'], row['profile'])
            if key in seen:
                raise ValueError('duplicate case/profile across shards: '+str(key))
            seen.add(key)
            result['rows'].append(row)
        result['omissions'].extend(data['omissions'])
    if result is None:
        raise ValueError('no input artifacts')
    result['omissions'] = sorted(set(result['omissions']))
    result['provenance'] = dict(result['provenance'], invocations=canonical(sources))
    result['rows'].sort(key=lambda r:(r['dataset']['ID'], r['case']['ID'], r['profile']))
    return result


def render_compact(data):
    return Path(__file__).with_name('template.html').read_text().replace('__REPORT_DATA__', render.safe_json(data))


def summary(data):
    rows = []
    for row in data['rows']:
        rows.append(dict(caseKey=case_key(row),key=comparison_key(data, row), dataset=row['dataset']['ID'], case=row['case']['ID'],
                         profile=row['profile'], status=row['status'], relationships=row['dataset']['Relationships'],
                         family=row['dataset'].get('Family',''),inputBytes=row['dataset'].get('InputBytes',0),database=row['dataset'].get('Database'),
                         engines=[dict(name=e['name'], time=e['metrics']['time'], work=e['work'], fingerprint=e.get('workFingerprint')) for e in row['engines']]))
    return dict(created=data['created'], source=data['provenance']['commit'], policy=data['policy'], environment=environment(data),
                datasets=len(set(r['dataset'] for r in rows)), comparisons=len(rows), maxRelationships=max(r['relationships'] for r in rows),
                samples=data['samples'], rows=rows)


def archive(root, run_id, title, inputs, existing_report=None, extras=None):
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*', run_id):
        raise ValueError('run ID must be a safe directory name')
    root = Path(root); runs = root/'runs'; runs.mkdir(parents=True, exist_ok=True)
    final = runs/run_id
    if final.exists():
        raise FileExistsError('archived run already exists: '+str(final))
    # All compression/rendering happens after measurement. Stage everything before
    # the new directory becomes visible to the index; never replace an existing run.
    stage = Path(tempfile.mkdtemp(prefix='.staging-', dir=root))
    try:
        data = merge_inputs(load(p) for p in inputs)
        (stage/'raw').mkdir()
        for i, source in enumerate(inputs):
            source = Path(source); target = stage/'raw'/f'{i:03d}.json.gz'
            if source.suffix == '.gz':
                shutil.copyfile(source, target)
            else:
                with source.open('rb') as src, gzip.GzipFile(filename=str(target),mode='wb',mtime=0) as dst:
                    shutil.copyfileobj(src,dst)
        if extras:
            (stage/'logs').mkdir()
            for source in extras:
                source=Path(source);target=stage/'logs'/source.name
                if target.exists():raise ValueError('duplicate log filename: '+source.name)
                shutil.copyfile(source,target)
        (stage/'data.json').write_text(render.safe_json(data))
        if existing_report:
            # Preserve an already-viewed report byte for byte.
            shutil.copyfile(existing_report, stage/'report.html')
        else:
            (stage/'report.html').write_text(render_compact(data))
        (stage/'summary.json').write_text(canonical(summary(data)))
        try:
            renderer = subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
        except (OSError, subprocess.CalledProcessError):
            renderer = 'unknown'
        manifest = dict(version=1,id=run_id,title=title,created=data['created'],measurementSource=data['provenance']['commit'],rendererSource=renderer,
                        rendererSHA256=digest(Path(__file__).with_name('template.html')),
                        files={str(p.relative_to(stage)):digest(p) for p in sorted(stage.rglob('*')) if p.is_file()})
        (stage/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
        # mkdir is the exclusivity gate even if two publishers race.
        final.mkdir()
        try:
            os.replace(stage, final)
        except BaseException:
            shutil.rmtree(final)
            raise
        rebuild(root)
        return final
    finally:
        shutil.rmtree(stage,ignore_errors=True)


def verify(run):
    run=Path(run);m=json.loads((run/'manifest.json').read_text())
    for name,expected in m['files'].items():
        path=run/name
        if not path.is_relative_to(run) or '..' in Path(name).parts or not path.is_file() or digest(path)!=expected:
            raise ValueError('archive checksum mismatch: '+name)
    return m


def rebuild(root):
    root=Path(root)
    with file_lock(root/'.index.lock'):
        return rebuild_locked(root)

def rebuild_locked(root):
    entries=[]
    for path in sorted((root/'runs').glob('*/manifest.json')):
        run=path.parent;m=verify(run)
        entries.append(dict(id=m['id'],title=m['title'],**summary(json.loads((run/'data.json').read_text()))))
    text=Path(__file__).with_name('history.html').read_text().replace('__HISTORY_DATA__',render.safe_json(entries))
    with tempfile.NamedTemporaryFile(mode='w',dir=root,prefix='.index-',suffix='.tmp',delete=False) as f:
        f.write(text);temp=Path(f.name)
    try:os.replace(temp,root/'index.html')
    finally:temp.unlink(missing_ok=True)
    return entries


def main():
    parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest='command',required=True)
    add=sub.add_parser('archive');add.add_argument('--root',type=Path,required=True);add.add_argument('--id',required=True);add.add_argument('--title',required=True);add.add_argument('--input',type=Path,action='append',required=True);add.add_argument('--existing-report',type=Path)
    index=sub.add_parser('index');index.add_argument('--root',type=Path,required=True)
    check=sub.add_parser('verify');check.add_argument('run',type=Path)
    args=parser.parse_args()
    if args.command=='archive':print(archive(args.root,args.id,args.title,args.input,args.existing_report))
    elif args.command=='index':print('Indexed',len(rebuild(args.root)),'runs')
    else:print('Verified',verify(args.run)['id'])

if __name__=='__main__':main()
