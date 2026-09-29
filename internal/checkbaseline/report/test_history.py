import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0, str(Path(__file__).parent))
import history


def artifact():
    sample = dict(NSPerOp=1000, BytesPerOp=20, AllocsPerOp=1, Iterations=2)
    engine = dict(Name='classic', Decision={'Outcome':'allow'}, Work=[{'Events':[]}], Samples=[sample]*10, Preparation=sample)
    qp = copy.deepcopy(engine); qp['Name']='qp'
    return dict(Version=1, Created='2026-09-29T00:00:00Z', Policy={'Version':1}, Repetitions=1, Samples=10,
                Provenance={'commit':'test', 'dirty':'','go':'go1','os':'test','arch':'test','cpu_model':'cpu','gomaxprocs':'1','classic':'serial','qp':'baseline','gogc':'100','gomemlimit':'off','godebug':'','build_settings':'{}'},
                Inputs={}, Omissions=[], Results=[dict(Dataset={'ID':'fixture','Family':'test','Hash':'abc','Relationships':1,'Resources':1,'Subjects':1,'InputBytes':20,'SchemaBytes':1},
                Case={'ID':'hit','Query':{'ResourceID':'d'},'Expected':{'Outcome':'allow'},'ClassicDepth':50,'QPDepth':50},Profile='memdb',Valid=True,Status='relationship work matched',Differences=[],Engines=[engine,qp])])

class HistoryTests(unittest.TestCase):
    def test_archive_is_immutable_and_checksums_detect_tampering(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); source=root/'input.json';source.write_text(json.dumps(artifact()))
            run=history.archive(root/'history','first','First run',[source])
            self.assertTrue((run/'report.html').exists())
            history.verify(run)
            original=(run/'report.html').read_bytes()
            with self.assertRaises(FileExistsError):history.archive(root/'history','first','Overwrite',[source])
            self.assertEqual((run/'report.html').read_bytes(),original)
            (run/'report.html').write_text('changed')
            with self.assertRaises(ValueError):history.verify(run)
    def test_comparison_identity_covers_input_request_policy_and_machine(self):
        a=history.merge_inputs([artifact()]);b=copy.deepcopy(a)
        self.assertEqual(history.comparison_key(a,a['rows'][0]),history.comparison_key(b,b['rows'][0]))
        for field in ['Hash','ID']:
            b=copy.deepcopy(a);b['rows'][0]['dataset'][field]='changed'
            self.assertNotEqual(history.comparison_key(a,a['rows'][0]),history.comparison_key(b,b['rows'][0]))
        for section,field in [('policy','Version'),('provenance','cpu_model'),('provenance','qp'),('provenance','gogc'),('provenance','build_settings'),('provenance','schema_mode'),('provenance','schema_cache')]:
            b=copy.deepcopy(a);b[section][field]='changed'
            self.assertNotEqual(history.comparison_key(a,a['rows'][0]),history.comparison_key(b,b['rows'][0]))
        b=copy.deepcopy(a);b['rows'][0]['case']['Query']['ResourceID']='other'
        self.assertNotEqual(history.comparison_key(a,a['rows'][0]),history.comparison_key(b,b['rows'][0]))
    def test_merge_rejects_duplicate_cases_and_conflicting_source(self):
        a=artifact()
        with self.assertRaises(ValueError):history.merge_inputs([a,a])
        b=copy.deepcopy(a);b['Results'][0]['Dataset']['ID']='other';b['Provenance']['commit']='other'
        with self.assertRaises(ValueError):history.merge_inputs([a,b])
    def test_invalid_or_partial_measurements_cannot_publish(self):
        for change in ['invalid','partial']:
            a=artifact()
            if change=='invalid':a['Results'][0]['Valid']=False
            else:a['Results'][0]['Engines'][0]['Samples']=[]
            with self.assertRaises(ValueError):history.merge_inputs([a])
    def test_unknown_cpu_or_runtime_settings_withhold_comparison_keys(self):
        data=history.merge_inputs([artifact()])
        for field in ['cpu_model','gogc','gomemlimit','build_settings']:
            candidate=copy.deepcopy(data);candidate['provenance'].pop(field)
            self.assertIsNone(history.comparison_key(candidate,candidate['rows'][0]))
    def test_exclusive_run_lock_rejects_second_owner(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'run.lock'
            with history.file_lock(path,blocking=False):
                with self.assertRaises(BlockingIOError):
                    with history.file_lock(path,blocking=False): pass
            with history.file_lock(path,blocking=False): pass
    def test_interrupted_log_is_not_overwritten(self):
        import run_suite
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);log=root/'000.log';log.write_text('interrupted attempt evidence')
            with self.assertRaises(FileExistsError):run_suite.ensure_new_shard(root/'000.json',root/'000.json.gz',log)
            self.assertEqual(log.read_text(),'interrupted attempt evidence')
    def test_concurrent_index_rebuilds_are_serialized(self):
        from concurrent.futures import ThreadPoolExecutor
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);source=root/'input.json';source.write_text(json.dumps(artifact()))
            history.archive(root/'history','first','First',[source])
            with ThreadPoolExecutor(max_workers=2) as workers:
                results=list(workers.map(lambda _:history.rebuild(root/'history'),range(4)))
            self.assertTrue(all(len(r)==1 for r in results))
            self.assertIn('First',(root/'history'/'index.html').read_text())
    def test_incomplete_postgres_configuration_cannot_publish_or_compare(self):
        for settings in ['', '{}', '{"shared_buffers":"512MB"}']:
            a=artifact();a['Provenance'].update(backend='postgres',backend_version='17.2',backend_settings=settings)
            data=history.render.compact(a)
            self.assertFalse(history.configuration_known(data))
            with self.assertRaises(ValueError):history.merge_inputs([a])

    def test_schema_loads_are_separate_and_unrecorded_history_is_unknown(self):
        a=artifact()
        for engine in a['Results'][0]['Engines']:engine['Work'][0]['Events']=[dict(Operation='schema-load')]
        self.assertIsNone(history.render.compact(a)['rows'][0]['engines'][0]['metrics']['schemaLoads'])
        a['Provenance']['schema_load_instrumentation']='datastore reader v1'
        metrics=history.render.compact(a)['rows'][0]['engines'][0]['metrics']
        self.assertEqual(metrics['schemaLoads']['median'],1)
        self.assertEqual(metrics['schema']['median'],0)

    def test_sql_metadata_and_combined_chart_preserve_size_dimensions(self):
        a=artifact();settings={k:'recorded' for k in ('shared_buffers','work_mem','effective_cache_size','fsync','autovacuum','jit','plan_cache_mode','enable_indexscan','enable_indexonlyscan','enable_bitmapscan','enable_seqscan','pool_read_max','pool_write_max','cache_policy','preparation')}
        settings['container']=json.dumps(dict(image='sha256:test',architecture='arm64',dockerVersion='29',transport='loopback',cpus=4,memoryBytes=100,vmCPUs=10,vmMemoryBytes=200))
        a['Provenance'].update(backend='postgres',backend_version='17.2',backend_settings=json.dumps(settings))
        a['Results'][0]['Profile']='postgres'
        a['Results'][0]['Dataset']['Database']={'TableBytes':8192,'IndexBytes':16384,'DatabaseBytes':100000,'RelationshipRows':1}
        summary=history.summary(history.merge_inputs([a]))
        row=summary['rows'][0]
        self.assertEqual(row['inputBytes'],20)
        self.assertEqual(row['family'],'test')
        self.assertEqual(row['database']['TableBytes'],8192)
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);source=root/'input.json';source.write_text(json.dumps(a))
            history.archive(root/'history','postgres','Postgres',[source])
            html=(root/'history'/'index.html').read_text()
            self.assertIn('id="combinedChart"',html)
            self.assertIn('id="graphProfile"',html)

    def test_run_ids_cannot_escape_history_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(ValueError):history.archive(Path(temp),'../escape','bad',[])

if __name__=='__main__':unittest.main()
