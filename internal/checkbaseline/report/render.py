#!/usr/bin/env python3
"""Render local checkbaseline JSON as a portable, offline HTML report."""
import argparse
import json
import math
from pathlib import Path


def summarize(values):
    values = sorted(values)
    if not values:
        return None
    if any(not math.isfinite(v) for v in values):
        raise ValueError('non-finite measurement')
    def q(f):
        i = (len(values) - 1) * f
        lo = int(i)
        hi = min(lo + 1, len(values) - 1)
        return values[lo] + (values[hi] - values[lo]) * (i - lo)
    return dict(n=len(values), low=values[0], q1=q(.25), median=q(.5), q3=q(.75), high=values[-1])


def paired_ratio(classic, qp):
    if len(classic) != len(qp):
        raise ValueError('unpaired samples')
    return summarize([b / a for a, b in zip(classic, qp) if a > 0])


def safe_json(data):
    return json.dumps(data, separators=(',', ':'), allow_nan=False).replace('<', '\\u003c').replace('>', '\\u003e').replace('&', '\\u0026')


def work_totals(work):
    events = work.get('Events') or []
    loads = [e for e in events if e['Operation'] in ('query', 'reverse')]
    return dict(loads=len(loads), rows=sum(e['Rows'] for e in loads), payload=sum(e['Bytes'] for e in loads),
                schema=sum(e['Operation'] not in ('query', 'reverse', 'caveat-leaf', 'caveat-expression','schema-load') for e in events),
                caveats=sum(e['Operation'] == 'caveat-leaf' for e in events),
                batch=max([e['Batch'] for e in loads], default=0))


def compact(artifact):
    if artifact['Version'] != 1:
        raise ValueError('unsupported artifact version')
    rows = []
    for result in artifact['Results']:
        engines = []
        for e in result['Engines']:
            samples = e.get('Samples') or []
            work = [work_totals(w) for w in e['Work']]
            metrics = {key:summarize([w[key] for w in work]) for key in ('loads','rows','payload','schema','caveats','batch')}
            metrics['schemaLoads']=summarize([sum(e['Operation']=='schema-load' for e in w.get('Events') or []) for w in e['Work']]) if artifact['Provenance'].get('schema_load_instrumentation') else None
            metrics.update(time=summarize([s['NSPerOp']/1000 for s in samples]),
                           allocations=summarize([s['AllocsPerOp'] for s in samples]),
                           memory=summarize([s['BytesPerOp'] for s in samples]))
            prep = e.get('Preparation') or {}
            metrics.update(prepareTime=summarize([prep.get('NSPerOp', 0)/1000]),
                           prepareMemory=summarize([prep.get('BytesPerOp', 0)]))
            events = (e['Work'][0].get('Events') or []) if e['Work'] else []
            engines.append(dict(name=e['Name'], metrics=metrics, decision=e['Decision'], error=e.get('Error'),
                                preparation=e.get('Preparation'), samples=samples, observations=e.get('Observations'),
                                trace=[{k:v for k,v in event.items() if k!='Relationships'} for event in events[:60]],
                                traceCount=len(events), work=work))
        ratio = None
        if result['Valid'] and result['Status'] == 'relationship work matched':
            ratio = paired_ratio([s['NSPerOp'] for s in engines[0]['samples']], [s['NSPerOp'] for s in engines[1]['samples']])
        rows.append(dict(dataset=result['Dataset'], case=result['Case'], profile=result['Profile'], valid=result['Valid'],
                         status=result['Status'], differences=result.get('Differences') or [], engines=engines, ratio=ratio))
    return dict(created=artifact['Created'], provenance=artifact['Provenance'], policy=artifact['Policy'],
                repetitions=artifact['Repetitions'], samples=artifact['Samples'], omissions=artifact.get('Omissions') or [], rows=rows)


def render(artifact):
    return Path(__file__).with_name('template.html').read_text().replace('__REPORT_DATA__', safe_json(compact(artifact)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input',type=Path)
    parser.add_argument('output',type=Path)
    args=parser.parse_args()
    with args.input.open() as f:
        artifact=json.load(f)
    args.output.write_text(render(artifact))
    print(f'Wrote {args.output}: {len(artifact["Results"])} comparisons')

if __name__=='__main__': main()
