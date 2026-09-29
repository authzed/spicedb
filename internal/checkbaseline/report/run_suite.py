#!/usr/bin/env python3
"""Measure local datasets sequentially and publish one immutable report run."""
import argparse
import datetime
import gzip
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
import history


def ensure_new_shard(*paths):
    for path in paths:
        if Path(path).exists():
            raise FileExistsError('incomplete attempt requires inspection; preserving '+str(path))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root',type=Path,default=Path('artifacts/check-baseline/history'))
    p.add_argument('--id',required=True);p.add_argument('--title',required=True)
    p.add_argument('--catalog',choices=['baseline','scaling'],default='scaling')
    p.add_argument('--dataset',default='.*');p.add_argument('--profile',choices=['both','memdb','delay','postgres'],default='both')
    p.add_argument('--evidence',type=Path,action='append',default=[],help='local environment/test evidence to preserve with the report')
    p.add_argument('--experimental-schema-mode',default='read-legacy-write-legacy',choices=['read-legacy-write-legacy','read-legacy-write-both','read-new-write-both','read-new-write-new'])
    p.add_argument('--backend',choices=['memdb','postgres'],default='memdb')
    p.add_argument('--go',required=True);p.add_argument('--samples',type=int,default=10)
    p.add_argument('--repetitions',type=int,default=3);p.add_argument('--resume',action='store_true')
    args=p.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*',args.id):p.error('invalid run ID')
    args.root=args.root.resolve();args.root.mkdir(parents=True,exist_ok=True)
    if (args.root/'runs'/args.id).exists():p.error('run already published; choose a new ID')
    with history.file_lock(args.root/('.run-'+args.id+'.lock'),blocking=False):
        execute(args,p)


def execute(args,p):
    source=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
    dirty=subprocess.check_output(['git','status','--porcelain'],text=True).strip()
    if dirty:p.error('commit source changes before measuring: '+dirty)
    build=json.loads(subprocess.check_output([args.go,'env','-json','GOFLAGS','GOEXPERIMENT','CGO_ENABLED','GOOS','GOARCH'],text=True,env=dict(os.environ,GOTOOLCHAIN='local',GOPROXY='off',GOSUMDB='off')))
    runtime={k:os.environ.get(k,default) for k,default in [('GOGC','100'),('GOMEMLIMIT','off'),('GODEBUG',''),('GOMAXPROCS','auto')]}
    if args.backend=='postgres':
        for key in ('PGOPTIONS','PGSERVICE','PGSERVICEFILE'):
            if os.environ.get(key):p.error(key+' is unsupported for reproducible PostgreSQL benchmarks')
        if not os.environ.get('CHECKBASELINE_POSTGRES_URI'):p.error('CHECKBASELINE_POSTGRES_URI is required')
        if not os.environ.get('CHECKBASELINE_BACKEND_METADATA'):p.error('CHECKBASELINE_BACKEND_METADATA must record the isolated server configuration')
        if args.profile=='both':args.profile='postgres'
    config=dict(schemaMode=args.experimental_schema_mode,evidence={str(p.resolve()):history.digest(p) for p in args.evidence},backend=args.backend,backendMetadata=os.environ.get('CHECKBASELINE_BACKEND_METADATA',''),source=source,build=build,runtime=runtime,catalog=args.catalog,dataset=args.dataset,profile=args.profile,samples=args.samples,repetitions=args.repetitions)
    pending=args.root/('.pending-'+args.id)
    if pending.exists():
        if not args.resume:p.error('pending run exists; use --resume with the same configuration')
        if json.loads((pending/'config.json').read_text())!=config:p.error('resume configuration/source does not match')
    else:
        pending.mkdir();(pending/'config.json').write_text(json.dumps(config,indent=2)+'\n')
    env=dict(os.environ,GOTOOLCHAIN='local',GOPROXY='off',GOSUMDB='off')
    binary=pending/'checkbaseline'
    subprocess.run([args.go,'build','-o',str(binary),'./cmd/checkbaseline'],env=env,check=True)
    names=json.loads(subprocess.check_output([str(binary),'-catalog='+args.catalog,'-dataset='+args.dataset,'-list'],env=env,text=True))
    if not names:p.error('no selected datasets')
    shards=[];logs=[]
    for i,name in enumerate(names):
        raw=pending/f'{i:03d}.json';compressed=pending/f'{i:03d}.json.gz';log=pending/f'{i:03d}.log';receipt=pending/f'{i:03d}.receipt.json'
        if compressed.exists() and receipt.exists():
            saved=json.loads(receipt.read_text())
            if saved.get('dataset')!=name or saved.get('sha256')!=history.digest(compressed):p.error('resume shard failed integrity check: '+name)
            print(f'[{i+1}/{len(names)}] verified completed {name}',flush=True)
        else:
            ensure_new_shard(raw,compressed,log)
            command=[str(binary),'-mode=measure','-repo-root=.','-catalog='+args.catalog,'-dataset=^'+re.escape(name)+'$',
                     '-experimental-schema-mode='+args.experimental_schema_mode,'-backend='+args.backend,'-profile='+args.profile,'-samples='+str(args.samples),'-repetitions='+str(args.repetitions),'-output='+str(raw)]
            print(f'[{i+1}/{len(names)}] measuring {name}',flush=True);start=time.monotonic()
            with log.open('w') as output:
                output.write(json.dumps(command)+'\n');output.flush()
                completed=subprocess.run(command,env=env,stdout=output,stderr=subprocess.STDOUT)
            if completed.returncode:
                raise RuntimeError(f'{name} failed; partial observations/log retained at {pending}')
            with raw.open('rb') as src,gzip.GzipFile(filename=str(compressed),mode='wb',compresslevel=6,mtime=0) as dst:shutil.copyfileobj(src,dst)
            receipt.write_text(json.dumps(dict(dataset=name,sha256=history.digest(compressed),elapsedSeconds=time.monotonic()-start),indent=2)+'\n')
            raw.unlink() # Only this helper's newly generated, successfully compressed shard.
            print(f'[{i+1}/{len(names)}] completed {name} in {time.monotonic()-start:.1f}s',flush=True)
        shards.append(compressed);logs.extend([log,receipt])
    # Measurement is over before compression validation, combined rendering and publishing.
    manifest=pending/'invocations.json';manifest.write_text(json.dumps(dict(config=config,binarySHA256=history.digest(binary),datasets=names,finished=datetime.datetime.now(datetime.timezone.utc).isoformat()),indent=2)+'\n')
    run=history.archive(args.root,args.id,args.title,shards,extras=[manifest,*logs,*args.evidence])
    print('Published '+str(run/'report.html'),flush=True)
    print('History '+str(args.root/'index.html'),flush=True)

if __name__=='__main__':main()
