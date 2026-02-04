window.BENCHMARK_DATA = {
  "lastUpdate": 1770227337979,
  "repoUrl": "https://github.com/authzed/spicedb",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "maria.ines.parnisari@authzed.com",
            "name": "Maria Ines Parnisari",
            "username": "miparnisari"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5e4a3ccd32b5ca38ad081a1804814133b1820be9",
          "message": "ci: publish benchmarks (#2866)",
          "timestamp": "2026-01-29T20:49:21-03:00",
          "tree_id": "3c7f0e056f4a93009d0ec0d0b40c2a7b7f0bafd5",
          "url": "https://github.com/authzed/spicedb/commit/5e4a3ccd32b5ca38ad081a1804814133b1820be9"
        },
        "date": 1769731319604,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead",
            "value": 371973,
            "unit": "ns/op\t   15343 B/op\t     221 allocs/op",
            "extra": "16312 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead - ns/op",
            "value": 371973,
            "unit": "ns/op",
            "extra": "16312 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead - B/op",
            "value": 15343,
            "unit": "B/op",
            "extra": "16312 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead - allocs/op",
            "value": 221,
            "unit": "allocs/op",
            "extra": "16312 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace",
            "value": 8002373,
            "unit": "ns/op\t   94196 B/op\t   20135 allocs/op",
            "extra": "738 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace - ns/op",
            "value": 8002373,
            "unit": "ns/op",
            "extra": "738 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace - B/op",
            "value": 94196,
            "unit": "B/op",
            "extra": "738 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace - allocs/op",
            "value": 20135,
            "unit": "allocs/op",
            "extra": "738 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource",
            "value": 8079224,
            "unit": "ns/op\t   97336 B/op\t   20194 allocs/op",
            "extra": "756 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - ns/op",
            "value": 8079224,
            "unit": "ns/op",
            "extra": "756 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - B/op",
            "value": 97336,
            "unit": "B/op",
            "extra": "756 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "756 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject",
            "value": 10125845,
            "unit": "ns/op\t   97334 B/op\t   20194 allocs/op",
            "extra": "592 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - ns/op",
            "value": 10125845,
            "unit": "ns/op",
            "extra": "592 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - B/op",
            "value": 97334,
            "unit": "B/op",
            "extra": "592 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "592 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource",
            "value": 7843509,
            "unit": "ns/op\t   77818 B/op\t   15209 allocs/op",
            "extra": "759 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource - ns/op",
            "value": 7843509,
            "unit": "ns/op",
            "extra": "759 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource - B/op",
            "value": 77818,
            "unit": "B/op",
            "extra": "759 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource - allocs/op",
            "value": 15209,
            "unit": "allocs/op",
            "extra": "759 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject",
            "value": 9691370,
            "unit": "ns/op\t   77876 B/op\t   15210 allocs/op",
            "extra": "607 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject - ns/op",
            "value": 9691370,
            "unit": "ns/op",
            "extra": "607 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject - B/op",
            "value": 77876,
            "unit": "B/op",
            "extra": "607 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject - allocs/op",
            "value": 15210,
            "unit": "allocs/op",
            "extra": "607 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject",
            "value": 513824,
            "unit": "ns/op\t   18431 B/op\t     280 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - ns/op",
            "value": 513824,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - B/op",
            "value": 18431,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource",
            "value": 489347,
            "unit": "ns/op\t   18428 B/op\t     280 allocs/op",
            "extra": "12192 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - ns/op",
            "value": 489347,
            "unit": "ns/op",
            "extra": "12192 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - B/op",
            "value": 18428,
            "unit": "B/op",
            "extra": "12192 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "12192 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead",
            "value": 7693417,
            "unit": "ns/op\t  172688 B/op\t   20202 allocs/op",
            "extra": "776 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead - ns/op",
            "value": 7693417,
            "unit": "ns/op",
            "extra": "776 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead - B/op",
            "value": 172688,
            "unit": "B/op",
            "extra": "776 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead - allocs/op",
            "value": 20202,
            "unit": "allocs/op",
            "extra": "776 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch",
            "value": 11115737,
            "unit": "ns/op\t   23332 B/op\t     296 allocs/op",
            "extra": "565 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch - ns/op",
            "value": 11115737,
            "unit": "ns/op",
            "extra": "565 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch - B/op",
            "value": 23332,
            "unit": "B/op",
            "extra": "565 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch - allocs/op",
            "value": 296,
            "unit": "allocs/op",
            "extra": "565 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create",
            "value": 12493298,
            "unit": "ns/op\t   20794 B/op\t     291 allocs/op",
            "extra": "520 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create - ns/op",
            "value": 12493298,
            "unit": "ns/op",
            "extra": "520 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create - B/op",
            "value": 20794,
            "unit": "B/op",
            "extra": "520 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create - allocs/op",
            "value": 291,
            "unit": "allocs/op",
            "extra": "520 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_",
            "value": 19021979,
            "unit": "ns/op\t 4465378 B/op\t   45847 allocs/op",
            "extra": "307 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_ - ns/op",
            "value": 19021979,
            "unit": "ns/op",
            "extra": "307 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_ - B/op",
            "value": 4465378,
            "unit": "B/op",
            "extra": "307 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_ - allocs/op",
            "value": 45847,
            "unit": "allocs/op",
            "extra": "307 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_",
            "value": 29063020,
            "unit": "ns/op\t 4512225 B/op\t   45983 allocs/op",
            "extra": "218 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_ - ns/op",
            "value": 29063020,
            "unit": "ns/op",
            "extra": "218 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_ - B/op",
            "value": 4512225,
            "unit": "B/op",
            "extra": "218 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_ - allocs/op",
            "value": 45983,
            "unit": "allocs/op",
            "extra": "218 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_",
            "value": 33292100,
            "unit": "ns/op\t 4442369 B/op\t   46444 allocs/op",
            "extra": "169 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_ - ns/op",
            "value": 33292100,
            "unit": "ns/op",
            "extra": "169 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_ - B/op",
            "value": 4442369,
            "unit": "B/op",
            "extra": "169 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_ - allocs/op",
            "value": 46444,
            "unit": "allocs/op",
            "extra": "169 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_",
            "value": 44395016,
            "unit": "ns/op\t 4655978 B/op\t   46957 allocs/op",
            "extra": "135 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_ - ns/op",
            "value": 44395016,
            "unit": "ns/op",
            "extra": "135 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_ - B/op",
            "value": 4655978,
            "unit": "B/op",
            "extra": "135 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_ - allocs/op",
            "value": 46957,
            "unit": "allocs/op",
            "extra": "135 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_",
            "value": 62900081,
            "unit": "ns/op\t 5167308 B/op\t   48111 allocs/op",
            "extra": "96 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_ - ns/op",
            "value": 62900081,
            "unit": "ns/op",
            "extra": "96 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_ - B/op",
            "value": 5167308,
            "unit": "B/op",
            "extra": "96 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_ - allocs/op",
            "value": 48111,
            "unit": "allocs/op",
            "extra": "96 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead",
            "value": 369159,
            "unit": "ns/op\t   15342 B/op\t     221 allocs/op",
            "extra": "16246 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead - ns/op",
            "value": 369159,
            "unit": "ns/op",
            "extra": "16246 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead - B/op",
            "value": 15342,
            "unit": "B/op",
            "extra": "16246 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead - allocs/op",
            "value": 221,
            "unit": "allocs/op",
            "extra": "16246 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace",
            "value": 8100392,
            "unit": "ns/op\t   94271 B/op\t   20135 allocs/op",
            "extra": "730 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace - ns/op",
            "value": 8100392,
            "unit": "ns/op",
            "extra": "730 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace - B/op",
            "value": 94271,
            "unit": "B/op",
            "extra": "730 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace - allocs/op",
            "value": 20135,
            "unit": "allocs/op",
            "extra": "730 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject",
            "value": 10206146,
            "unit": "ns/op\t   97325 B/op\t   20194 allocs/op",
            "extra": "588 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - ns/op",
            "value": 10206146,
            "unit": "ns/op",
            "extra": "588 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - B/op",
            "value": 97325,
            "unit": "B/op",
            "extra": "588 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "588 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource",
            "value": 8194723,
            "unit": "ns/op\t   97301 B/op\t   20194 allocs/op",
            "extra": "730 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - ns/op",
            "value": 8194723,
            "unit": "ns/op",
            "extra": "730 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - B/op",
            "value": 97301,
            "unit": "B/op",
            "extra": "730 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "730 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource",
            "value": 7993393,
            "unit": "ns/op\t   77794 B/op\t   15209 allocs/op",
            "extra": "748 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource - ns/op",
            "value": 7993393,
            "unit": "ns/op",
            "extra": "748 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource - B/op",
            "value": 77794,
            "unit": "B/op",
            "extra": "748 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource - allocs/op",
            "value": 15209,
            "unit": "allocs/op",
            "extra": "748 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject",
            "value": 9889254,
            "unit": "ns/op\t   77812 B/op\t   15209 allocs/op",
            "extra": "591 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject - ns/op",
            "value": 9889254,
            "unit": "ns/op",
            "extra": "591 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject - B/op",
            "value": 77812,
            "unit": "B/op",
            "extra": "591 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject - allocs/op",
            "value": 15209,
            "unit": "allocs/op",
            "extra": "591 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource",
            "value": 391544,
            "unit": "ns/op\t   18429 B/op\t     280 allocs/op",
            "extra": "15714 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - ns/op",
            "value": 391544,
            "unit": "ns/op",
            "extra": "15714 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - B/op",
            "value": 18429,
            "unit": "B/op",
            "extra": "15714 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "15714 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject",
            "value": 550496,
            "unit": "ns/op\t   18433 B/op\t     280 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - ns/op",
            "value": 550496,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - B/op",
            "value": 18433,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead",
            "value": 7752740,
            "unit": "ns/op\t  172716 B/op\t   20202 allocs/op",
            "extra": "765 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead - ns/op",
            "value": 7752740,
            "unit": "ns/op",
            "extra": "765 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead - B/op",
            "value": 172716,
            "unit": "B/op",
            "extra": "765 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead - allocs/op",
            "value": 20202,
            "unit": "allocs/op",
            "extra": "765 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch",
            "value": 2589548,
            "unit": "ns/op\t   20934 B/op\t     272 allocs/op",
            "extra": "1963 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch - ns/op",
            "value": 2589548,
            "unit": "ns/op",
            "extra": "1963 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch - B/op",
            "value": 20934,
            "unit": "B/op",
            "extra": "1963 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch - allocs/op",
            "value": 272,
            "unit": "allocs/op",
            "extra": "1963 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create",
            "value": 1401851,
            "unit": "ns/op\t   18286 B/op\t     266 allocs/op",
            "extra": "4483 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create - ns/op",
            "value": 1401851,
            "unit": "ns/op",
            "extra": "4483 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create - B/op",
            "value": 18286,
            "unit": "B/op",
            "extra": "4483 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create - allocs/op",
            "value": 266,
            "unit": "allocs/op",
            "extra": "4483 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_",
            "value": 16390549,
            "unit": "ns/op\t 4463349 B/op\t   45823 allocs/op",
            "extra": "360 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_ - ns/op",
            "value": 16390549,
            "unit": "ns/op",
            "extra": "360 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_ - B/op",
            "value": 4463349,
            "unit": "B/op",
            "extra": "360 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_ - allocs/op",
            "value": 45823,
            "unit": "allocs/op",
            "extra": "360 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_",
            "value": 26242038,
            "unit": "ns/op\t 4509915 B/op\t   45960 allocs/op",
            "extra": "231 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_ - ns/op",
            "value": 26242038,
            "unit": "ns/op",
            "extra": "231 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_ - B/op",
            "value": 4509915,
            "unit": "B/op",
            "extra": "231 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_ - allocs/op",
            "value": 45960,
            "unit": "allocs/op",
            "extra": "231 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_",
            "value": 30219863,
            "unit": "ns/op\t 4438952 B/op\t   46421 allocs/op",
            "extra": "180 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_ - ns/op",
            "value": 30219863,
            "unit": "ns/op",
            "extra": "180 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_ - B/op",
            "value": 4438952,
            "unit": "B/op",
            "extra": "180 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_ - allocs/op",
            "value": 46421,
            "unit": "allocs/op",
            "extra": "180 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_",
            "value": 44677553,
            "unit": "ns/op\t 4653392 B/op\t   46931 allocs/op",
            "extra": "133 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_ - ns/op",
            "value": 44677553,
            "unit": "ns/op",
            "extra": "133 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_ - B/op",
            "value": 4653392,
            "unit": "B/op",
            "extra": "133 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_ - allocs/op",
            "value": 46931,
            "unit": "allocs/op",
            "extra": "133 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_",
            "value": 66940453,
            "unit": "ns/op\t 5174841 B/op\t   48086 allocs/op",
            "extra": "78 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_ - ns/op",
            "value": 66940453,
            "unit": "ns/op",
            "extra": "78 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_ - B/op",
            "value": 5174841,
            "unit": "B/op",
            "extra": "78 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_ - allocs/op",
            "value": 48086,
            "unit": "allocs/op",
            "extra": "78 times"
          },
          {
            "name": "BenchmarkQueryRelationships",
            "value": 440151,
            "unit": "ns/op\t     696 B/op\t      18 allocs/op",
            "extra": "13650 times"
          },
          {
            "name": "BenchmarkQueryRelationships - ns/op",
            "value": 440151,
            "unit": "ns/op",
            "extra": "13650 times"
          },
          {
            "name": "BenchmarkQueryRelationships - B/op",
            "value": 696,
            "unit": "B/op",
            "extra": "13650 times"
          },
          {
            "name": "BenchmarkQueryRelationships - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "13650 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true",
            "value": 2647375,
            "unit": "ns/op\t 2245616 B/op\t   17080 allocs/op",
            "extra": "2289 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true - ns/op",
            "value": 2647375,
            "unit": "ns/op",
            "extra": "2289 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true - B/op",
            "value": 2245616,
            "unit": "B/op",
            "extra": "2289 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true - allocs/op",
            "value": 17080,
            "unit": "allocs/op",
            "extra": "2289 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false",
            "value": 2186288,
            "unit": "ns/op\t 2101433 B/op\t   14076 allocs/op",
            "extra": "2938 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false - ns/op",
            "value": 2186288,
            "unit": "ns/op",
            "extra": "2938 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false - B/op",
            "value": 2101433,
            "unit": "B/op",
            "extra": "2938 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false - allocs/op",
            "value": 14076,
            "unit": "allocs/op",
            "extra": "2938 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash",
            "value": 614.2,
            "unit": "ns/op\t     144 B/op\t       3 allocs/op",
            "extra": "9714775 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash - ns/op",
            "value": 614.2,
            "unit": "ns/op",
            "extra": "9714775 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash - B/op",
            "value": 144,
            "unit": "B/op",
            "extra": "9714775 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9714775 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions",
            "value": 138.7,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "43240776 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions - ns/op",
            "value": 138.7,
            "unit": "ns/op",
            "extra": "43240776 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "43240776 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "43240776 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching",
            "value": 26287,
            "unit": "ns/op\t    3976 B/op\t      72 allocs/op",
            "extra": "315044 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching - ns/op",
            "value": 26287,
            "unit": "ns/op",
            "extra": "315044 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching - B/op",
            "value": 3976,
            "unit": "B/op",
            "extra": "315044 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching - allocs/op",
            "value": 72,
            "unit": "allocs/op",
            "extra": "315044 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression",
            "value": 1982,
            "unit": "ns/op\t    1577 B/op\t      34 allocs/op",
            "extra": "3050626 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression - ns/op",
            "value": 1982,
            "unit": "ns/op",
            "extra": "3050626 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression - B/op",
            "value": 1577,
            "unit": "B/op",
            "extra": "3050626 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression - allocs/op",
            "value": 34,
            "unit": "allocs/op",
            "extra": "3050626 times"
          },
          {
            "name": "BenchmarkPatternMatcher",
            "value": 3406,
            "unit": "ns/op\t    5952 B/op\t      50 allocs/op",
            "extra": "1771137 times"
          },
          {
            "name": "BenchmarkPatternMatcher - ns/op",
            "value": 3406,
            "unit": "ns/op",
            "extra": "1771137 times"
          },
          {
            "name": "BenchmarkPatternMatcher - B/op",
            "value": 5952,
            "unit": "B/op",
            "extra": "1771137 times"
          },
          {
            "name": "BenchmarkPatternMatcher - allocs/op",
            "value": 50,
            "unit": "allocs/op",
            "extra": "1771137 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1",
            "value": 3794050,
            "unit": "ns/op\t  310398 B/op\t    4659 allocs/op",
            "extra": "2128 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1 - ns/op",
            "value": 3794050,
            "unit": "ns/op",
            "extra": "2128 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1 - B/op",
            "value": 310398,
            "unit": "B/op",
            "extra": "2128 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 4659,
            "unit": "allocs/op",
            "extra": "2128 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1",
            "value": 2015171,
            "unit": "ns/op\t  322504 B/op\t    4840 allocs/op",
            "extra": "3589 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1 - ns/op",
            "value": 2015171,
            "unit": "ns/op",
            "extra": "3589 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1 - B/op",
            "value": 322504,
            "unit": "B/op",
            "extra": "3589 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1 - allocs/op",
            "value": 4840,
            "unit": "allocs/op",
            "extra": "3589 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1",
            "value": 843114,
            "unit": "ns/op\t  232819 B/op\t    3593 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1 - ns/op",
            "value": 843114,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1 - B/op",
            "value": 232819,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1 - allocs/op",
            "value": 3593,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1",
            "value": 8443252,
            "unit": "ns/op\t  545320 B/op\t    7547 allocs/op",
            "extra": "700 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1 - ns/op",
            "value": 8443252,
            "unit": "ns/op",
            "extra": "700 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1 - B/op",
            "value": 545320,
            "unit": "B/op",
            "extra": "700 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1 - allocs/op",
            "value": 7547,
            "unit": "allocs/op",
            "extra": "700 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1",
            "value": 4131866,
            "unit": "ns/op\t  587005 B/op\t    8171 allocs/op",
            "extra": "1683 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1 - ns/op",
            "value": 4131866,
            "unit": "ns/op",
            "extra": "1683 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1 - B/op",
            "value": 587005,
            "unit": "B/op",
            "extra": "1683 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1 - allocs/op",
            "value": 8171,
            "unit": "allocs/op",
            "extra": "1683 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1",
            "value": 1143654,
            "unit": "ns/op\t  323067 B/op\t    4494 allocs/op",
            "extra": "7806 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1 - ns/op",
            "value": 1143654,
            "unit": "ns/op",
            "extra": "7806 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1 - B/op",
            "value": 323067,
            "unit": "B/op",
            "extra": "7806 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1 - allocs/op",
            "value": 4494,
            "unit": "allocs/op",
            "extra": "7806 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1",
            "value": 16999453,
            "unit": "ns/op\t 1973163 B/op\t   25398 allocs/op",
            "extra": "444 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1 - ns/op",
            "value": 16999453,
            "unit": "ns/op",
            "extra": "444 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1 - B/op",
            "value": 1973163,
            "unit": "B/op",
            "extra": "444 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1 - allocs/op",
            "value": 25398,
            "unit": "allocs/op",
            "extra": "444 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1",
            "value": 11200917,
            "unit": "ns/op\t 2095464 B/op\t   27006 allocs/op",
            "extra": "716 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1 - ns/op",
            "value": 11200917,
            "unit": "ns/op",
            "extra": "716 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1 - B/op",
            "value": 2095464,
            "unit": "B/op",
            "extra": "716 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1 - allocs/op",
            "value": 27006,
            "unit": "allocs/op",
            "extra": "716 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1",
            "value": 5225573,
            "unit": "ns/op\t 1259410 B/op\t   13549 allocs/op",
            "extra": "2056 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1 - ns/op",
            "value": 5225573,
            "unit": "ns/op",
            "extra": "2056 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1 - B/op",
            "value": 1259410,
            "unit": "B/op",
            "extra": "2056 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1 - allocs/op",
            "value": 13549,
            "unit": "allocs/op",
            "extra": "2056 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1",
            "value": 38998867,
            "unit": "ns/op\t 9932477 B/op\t  136533 allocs/op",
            "extra": "247 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1 - ns/op",
            "value": 38998867,
            "unit": "ns/op",
            "extra": "247 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1 - B/op",
            "value": 9932477,
            "unit": "B/op",
            "extra": "247 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1 - allocs/op",
            "value": 136533,
            "unit": "allocs/op",
            "extra": "247 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1",
            "value": 36636633,
            "unit": "ns/op\t10049482 B/op\t  140209 allocs/op",
            "extra": "172 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1 - ns/op",
            "value": 36636633,
            "unit": "ns/op",
            "extra": "172 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1 - B/op",
            "value": 10049482,
            "unit": "B/op",
            "extra": "172 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1 - allocs/op",
            "value": 140209,
            "unit": "allocs/op",
            "extra": "172 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1",
            "value": 28047414,
            "unit": "ns/op\t 9240643 B/op\t  125070 allocs/op",
            "extra": "206 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1 - ns/op",
            "value": 28047414,
            "unit": "ns/op",
            "extra": "206 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1 - B/op",
            "value": 9240643,
            "unit": "B/op",
            "extra": "206 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1 - allocs/op",
            "value": 125070,
            "unit": "allocs/op",
            "extra": "206 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1",
            "value": 4981414,
            "unit": "ns/op\t  296369 B/op\t    4517 allocs/op",
            "extra": "1437 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1 - ns/op",
            "value": 4981414,
            "unit": "ns/op",
            "extra": "1437 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1 - B/op",
            "value": 296369,
            "unit": "B/op",
            "extra": "1437 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 4517,
            "unit": "allocs/op",
            "extra": "1437 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1",
            "value": 1995941,
            "unit": "ns/op\t  318845 B/op\t    4847 allocs/op",
            "extra": "3446 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1 - ns/op",
            "value": 1995941,
            "unit": "ns/op",
            "extra": "3446 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1 - B/op",
            "value": 318845,
            "unit": "B/op",
            "extra": "3446 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1 - allocs/op",
            "value": 4847,
            "unit": "allocs/op",
            "extra": "3446 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1",
            "value": 490126,
            "unit": "ns/op\t  194560 B/op\t    3195 allocs/op",
            "extra": "12006 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1 - ns/op",
            "value": 490126,
            "unit": "ns/op",
            "extra": "12006 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1 - B/op",
            "value": 194560,
            "unit": "B/op",
            "extra": "12006 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1 - allocs/op",
            "value": 3195,
            "unit": "allocs/op",
            "extra": "12006 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1",
            "value": 10833095,
            "unit": "ns/op\t  953242 B/op\t   14963 allocs/op",
            "extra": "679 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1 - ns/op",
            "value": 10833095,
            "unit": "ns/op",
            "extra": "679 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1 - B/op",
            "value": 953242,
            "unit": "B/op",
            "extra": "679 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 14963,
            "unit": "allocs/op",
            "extra": "679 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1",
            "value": 4553412,
            "unit": "ns/op\t 1021761 B/op\t   16046 allocs/op",
            "extra": "1290 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1 - ns/op",
            "value": 4553412,
            "unit": "ns/op",
            "extra": "1290 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1 - B/op",
            "value": 1021761,
            "unit": "B/op",
            "extra": "1290 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1 - allocs/op",
            "value": 16046,
            "unit": "allocs/op",
            "extra": "1290 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1",
            "value": 1183162,
            "unit": "ns/op\t  636061 B/op\t   11246 allocs/op",
            "extra": "4699 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1 - ns/op",
            "value": 1183162,
            "unit": "ns/op",
            "extra": "4699 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1 - B/op",
            "value": 636061,
            "unit": "B/op",
            "extra": "4699 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1 - allocs/op",
            "value": 11246,
            "unit": "allocs/op",
            "extra": "4699 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1",
            "value": 13041247,
            "unit": "ns/op\t  585226 B/op\t    7858 allocs/op",
            "extra": "632 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1 - ns/op",
            "value": 13041247,
            "unit": "ns/op",
            "extra": "632 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1 - B/op",
            "value": 585226,
            "unit": "B/op",
            "extra": "632 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 7858,
            "unit": "allocs/op",
            "extra": "632 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1",
            "value": 3779867,
            "unit": "ns/op\t  630365 B/op\t    8328 allocs/op",
            "extra": "1543 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1 - ns/op",
            "value": 3779867,
            "unit": "ns/op",
            "extra": "1543 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1 - B/op",
            "value": 630365,
            "unit": "B/op",
            "extra": "1543 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1 - allocs/op",
            "value": 8328,
            "unit": "allocs/op",
            "extra": "1543 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1",
            "value": 711879,
            "unit": "ns/op\t  335013 B/op\t    4094 allocs/op",
            "extra": "7814 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1 - ns/op",
            "value": 711879,
            "unit": "ns/op",
            "extra": "7814 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1 - B/op",
            "value": 335013,
            "unit": "B/op",
            "extra": "7814 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1 - allocs/op",
            "value": 4094,
            "unit": "allocs/op",
            "extra": "7814 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1",
            "value": 3711947,
            "unit": "ns/op\t  134149 B/op\t    2031 allocs/op",
            "extra": "2616 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1 - ns/op",
            "value": 3711947,
            "unit": "ns/op",
            "extra": "2616 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1 - B/op",
            "value": 134149,
            "unit": "B/op",
            "extra": "2616 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1 - allocs/op",
            "value": 2031,
            "unit": "allocs/op",
            "extra": "2616 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1",
            "value": 2556056,
            "unit": "ns/op\t  140646 B/op\t    2106 allocs/op",
            "extra": "2361 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1 - ns/op",
            "value": 2556056,
            "unit": "ns/op",
            "extra": "2361 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1 - B/op",
            "value": 140646,
            "unit": "B/op",
            "extra": "2361 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1 - allocs/op",
            "value": 2106,
            "unit": "allocs/op",
            "extra": "2361 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1",
            "value": 207425,
            "unit": "ns/op\t   82909 B/op\t    1316 allocs/op",
            "extra": "28784 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1 - ns/op",
            "value": 207425,
            "unit": "ns/op",
            "extra": "28784 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1 - B/op",
            "value": 82909,
            "unit": "B/op",
            "extra": "28784 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1 - allocs/op",
            "value": 1316,
            "unit": "allocs/op",
            "extra": "28784 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph",
            "value": 7556,
            "unit": "ns/op\t    6408 B/op\t      89 allocs/op",
            "extra": "807764 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph - ns/op",
            "value": 7556,
            "unit": "ns/op",
            "extra": "807764 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph - B/op",
            "value": 6408,
            "unit": "B/op",
            "extra": "807764 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph - allocs/op",
            "value": 89,
            "unit": "allocs/op",
            "extra": "807764 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph",
            "value": 27222,
            "unit": "ns/op\t   25736 B/op\t     223 allocs/op",
            "extra": "225314 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph - ns/op",
            "value": 27222,
            "unit": "ns/op",
            "extra": "225314 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph - B/op",
            "value": 25736,
            "unit": "B/op",
            "extra": "225314 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph - allocs/op",
            "value": 223,
            "unit": "allocs/op",
            "extra": "225314 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph",
            "value": 24861,
            "unit": "ns/op\t   21640 B/op\t     251 allocs/op",
            "extra": "235712 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph - ns/op",
            "value": 24861,
            "unit": "ns/op",
            "extra": "235712 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph - B/op",
            "value": 21640,
            "unit": "B/op",
            "extra": "235712 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph - allocs/op",
            "value": 251,
            "unit": "allocs/op",
            "extra": "235712 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph",
            "value": 1933,
            "unit": "ns/op\t    1208 B/op\t      29 allocs/op",
            "extra": "3107296 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph - ns/op",
            "value": 1933,
            "unit": "ns/op",
            "extra": "3107296 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph - B/op",
            "value": 1208,
            "unit": "B/op",
            "extra": "3107296 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph - allocs/op",
            "value": 29,
            "unit": "allocs/op",
            "extra": "3107296 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph",
            "value": 32800,
            "unit": "ns/op\t   39217 B/op\t     222 allocs/op",
            "extra": "177440 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph - ns/op",
            "value": 32800,
            "unit": "ns/op",
            "extra": "177440 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph - B/op",
            "value": 39217,
            "unit": "B/op",
            "extra": "177440 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph - allocs/op",
            "value": 222,
            "unit": "allocs/op",
            "extra": "177440 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph",
            "value": 7622,
            "unit": "ns/op\t    6384 B/op\t      90 allocs/op",
            "extra": "781443 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph - ns/op",
            "value": 7622,
            "unit": "ns/op",
            "extra": "781443 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph - B/op",
            "value": 6384,
            "unit": "B/op",
            "extra": "781443 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph - allocs/op",
            "value": 90,
            "unit": "allocs/op",
            "extra": "781443 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources",
            "value": 8963,
            "unit": "ns/op\t    7112 B/op\t     111 allocs/op",
            "extra": "670430 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources - ns/op",
            "value": 8963,
            "unit": "ns/op",
            "extra": "670430 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources - B/op",
            "value": 7112,
            "unit": "B/op",
            "extra": "670430 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources - allocs/op",
            "value": 111,
            "unit": "allocs/op",
            "extra": "670430 times"
          },
          {
            "name": "BenchmarkRelationsReferencing",
            "value": 18.63,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "00000321948423 times"
          },
          {
            "name": "BenchmarkRelationsReferencing - ns/op",
            "value": 18.63,
            "unit": "ns/op",
            "extra": "00000321948423 times"
          },
          {
            "name": "BenchmarkRelationsReferencing - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "00000321948423 times"
          },
          {
            "name": "BenchmarkRelationsReferencing - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "00000321948423 times"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "barak.michener@authzed.com",
            "name": "Barak Michener",
            "username": "barakmich"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ce59421cc4d4ce23c0843b4c05ee8821222cfb20",
          "message": "feat: add type filtering to the query plan (#2850)",
          "timestamp": "2026-01-30T19:29:21Z",
          "tree_id": "64b7cb55d9741ca608a4067324476273490f8470",
          "url": "https://github.com/authzed/spicedb/commit/ce59421cc4d4ce23c0843b4c05ee8821222cfb20"
        },
        "date": 1769802103158,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead",
            "value": 375564,
            "unit": "ns/op\t   15342 B/op\t     221 allocs/op",
            "extra": "15858 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead - ns/op",
            "value": 375564,
            "unit": "ns/op",
            "extra": "15858 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead - B/op",
            "value": 15342,
            "unit": "B/op",
            "extra": "15858 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead - allocs/op",
            "value": 221,
            "unit": "allocs/op",
            "extra": "15858 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace",
            "value": 8029007,
            "unit": "ns/op\t   94244 B/op\t   20135 allocs/op",
            "extra": "739 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace - ns/op",
            "value": 8029007,
            "unit": "ns/op",
            "extra": "739 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace - B/op",
            "value": 94244,
            "unit": "B/op",
            "extra": "739 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace - allocs/op",
            "value": 20135,
            "unit": "allocs/op",
            "extra": "739 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource",
            "value": 8046098,
            "unit": "ns/op\t   97330 B/op\t   20194 allocs/op",
            "extra": "739 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - ns/op",
            "value": 8046098,
            "unit": "ns/op",
            "extra": "739 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - B/op",
            "value": 97330,
            "unit": "B/op",
            "extra": "739 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "739 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject",
            "value": 10174020,
            "unit": "ns/op\t   97324 B/op\t   20194 allocs/op",
            "extra": "590 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - ns/op",
            "value": 10174020,
            "unit": "ns/op",
            "extra": "590 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - B/op",
            "value": 97324,
            "unit": "B/op",
            "extra": "590 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "590 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource",
            "value": 7902414,
            "unit": "ns/op\t   77795 B/op\t   15209 allocs/op",
            "extra": "758 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource - ns/op",
            "value": 7902414,
            "unit": "ns/op",
            "extra": "758 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource - B/op",
            "value": 77795,
            "unit": "B/op",
            "extra": "758 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource - allocs/op",
            "value": 15209,
            "unit": "allocs/op",
            "extra": "758 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject",
            "value": 9657851,
            "unit": "ns/op\t   77807 B/op\t   15209 allocs/op",
            "extra": "614 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject - ns/op",
            "value": 9657851,
            "unit": "ns/op",
            "extra": "614 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject - B/op",
            "value": 77807,
            "unit": "B/op",
            "extra": "614 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject - allocs/op",
            "value": 15209,
            "unit": "allocs/op",
            "extra": "614 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource",
            "value": 513290,
            "unit": "ns/op\t   18431 B/op\t     280 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - ns/op",
            "value": 513290,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - B/op",
            "value": 18431,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject",
            "value": 516961,
            "unit": "ns/op\t   18429 B/op\t     280 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - ns/op",
            "value": 516961,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - B/op",
            "value": 18429,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead",
            "value": 7785313,
            "unit": "ns/op\t  172718 B/op\t   20202 allocs/op",
            "extra": "764 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead - ns/op",
            "value": 7785313,
            "unit": "ns/op",
            "extra": "764 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead - B/op",
            "value": 172718,
            "unit": "B/op",
            "extra": "764 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead - allocs/op",
            "value": 20202,
            "unit": "allocs/op",
            "extra": "764 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch",
            "value": 16038231,
            "unit": "ns/op\t   23358 B/op\t     297 allocs/op",
            "extra": "363 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch - ns/op",
            "value": 16038231,
            "unit": "ns/op",
            "extra": "363 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch - B/op",
            "value": 23358,
            "unit": "B/op",
            "extra": "363 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch - allocs/op",
            "value": 297,
            "unit": "allocs/op",
            "extra": "363 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create",
            "value": 16639945,
            "unit": "ns/op\t   20804 B/op\t     291 allocs/op",
            "extra": "345 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create - ns/op",
            "value": 16639945,
            "unit": "ns/op",
            "extra": "345 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create - B/op",
            "value": 20804,
            "unit": "B/op",
            "extra": "345 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create - allocs/op",
            "value": 291,
            "unit": "allocs/op",
            "extra": "345 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_",
            "value": 23697405,
            "unit": "ns/op\t 4466293 B/op\t   45848 allocs/op",
            "extra": "243 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_ - ns/op",
            "value": 23697405,
            "unit": "ns/op",
            "extra": "243 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_ - B/op",
            "value": 4466293,
            "unit": "B/op",
            "extra": "243 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_ - allocs/op",
            "value": 45848,
            "unit": "allocs/op",
            "extra": "243 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_",
            "value": 34206935,
            "unit": "ns/op\t 4511181 B/op\t   45984 allocs/op",
            "extra": "180 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_ - ns/op",
            "value": 34206935,
            "unit": "ns/op",
            "extra": "180 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_ - B/op",
            "value": 4511181,
            "unit": "B/op",
            "extra": "180 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_ - allocs/op",
            "value": 45984,
            "unit": "allocs/op",
            "extra": "180 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_",
            "value": 38238809,
            "unit": "ns/op\t 4442564 B/op\t   46443 allocs/op",
            "extra": "136 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_ - ns/op",
            "value": 38238809,
            "unit": "ns/op",
            "extra": "136 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_ - B/op",
            "value": 4442564,
            "unit": "B/op",
            "extra": "136 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_ - allocs/op",
            "value": 46443,
            "unit": "allocs/op",
            "extra": "136 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_",
            "value": 45012997,
            "unit": "ns/op\t 4655917 B/op\t   46957 allocs/op",
            "extra": "126 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_ - ns/op",
            "value": 45012997,
            "unit": "ns/op",
            "extra": "126 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_ - B/op",
            "value": 4655917,
            "unit": "B/op",
            "extra": "126 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_ - allocs/op",
            "value": 46957,
            "unit": "allocs/op",
            "extra": "126 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_",
            "value": 64286983,
            "unit": "ns/op\t 5173816 B/op\t   48111 allocs/op",
            "extra": "80 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_ - ns/op",
            "value": 64286983,
            "unit": "ns/op",
            "extra": "80 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_ - B/op",
            "value": 5173816,
            "unit": "B/op",
            "extra": "80 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_ - allocs/op",
            "value": 48111,
            "unit": "allocs/op",
            "extra": "80 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead",
            "value": 379208,
            "unit": "ns/op\t   15341 B/op\t     221 allocs/op",
            "extra": "15812 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead - ns/op",
            "value": 379208,
            "unit": "ns/op",
            "extra": "15812 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead - B/op",
            "value": 15341,
            "unit": "B/op",
            "extra": "15812 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead - allocs/op",
            "value": 221,
            "unit": "allocs/op",
            "extra": "15812 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace",
            "value": 8095878,
            "unit": "ns/op\t   94243 B/op\t   20135 allocs/op",
            "extra": "732 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace - ns/op",
            "value": 8095878,
            "unit": "ns/op",
            "extra": "732 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace - B/op",
            "value": 94243,
            "unit": "B/op",
            "extra": "732 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace - allocs/op",
            "value": 20135,
            "unit": "allocs/op",
            "extra": "732 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource",
            "value": 8087464,
            "unit": "ns/op\t   97304 B/op\t   20194 allocs/op",
            "extra": "740 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - ns/op",
            "value": 8087464,
            "unit": "ns/op",
            "extra": "740 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - B/op",
            "value": 97304,
            "unit": "B/op",
            "extra": "740 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "740 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject",
            "value": 10201543,
            "unit": "ns/op\t   97295 B/op\t   20194 allocs/op",
            "extra": "585 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - ns/op",
            "value": 10201543,
            "unit": "ns/op",
            "extra": "585 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - B/op",
            "value": 97295,
            "unit": "B/op",
            "extra": "585 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "585 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource",
            "value": 7948645,
            "unit": "ns/op\t   77799 B/op\t   15209 allocs/op",
            "extra": "752 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource - ns/op",
            "value": 7948645,
            "unit": "ns/op",
            "extra": "752 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource - B/op",
            "value": 77799,
            "unit": "B/op",
            "extra": "752 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource - allocs/op",
            "value": 15209,
            "unit": "allocs/op",
            "extra": "752 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject",
            "value": 9896097,
            "unit": "ns/op\t   77865 B/op\t   15210 allocs/op",
            "extra": "602 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject - ns/op",
            "value": 9896097,
            "unit": "ns/op",
            "extra": "602 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject - B/op",
            "value": 77865,
            "unit": "B/op",
            "extra": "602 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject - allocs/op",
            "value": 15210,
            "unit": "allocs/op",
            "extra": "602 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource",
            "value": 414988,
            "unit": "ns/op\t   18430 B/op\t     280 allocs/op",
            "extra": "15486 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - ns/op",
            "value": 414988,
            "unit": "ns/op",
            "extra": "15486 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - B/op",
            "value": 18430,
            "unit": "B/op",
            "extra": "15486 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "15486 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject",
            "value": 532208,
            "unit": "ns/op\t   18431 B/op\t     280 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - ns/op",
            "value": 532208,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - B/op",
            "value": 18431,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead",
            "value": 7773050,
            "unit": "ns/op\t  172690 B/op\t   20202 allocs/op",
            "extra": "768 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead - ns/op",
            "value": 7773050,
            "unit": "ns/op",
            "extra": "768 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead - B/op",
            "value": 172690,
            "unit": "B/op",
            "extra": "768 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead - allocs/op",
            "value": 20202,
            "unit": "allocs/op",
            "extra": "768 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch",
            "value": 2994448,
            "unit": "ns/op\t   20953 B/op\t     272 allocs/op",
            "extra": "2012 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch - ns/op",
            "value": 2994448,
            "unit": "ns/op",
            "extra": "2012 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch - B/op",
            "value": 20953,
            "unit": "B/op",
            "extra": "2012 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch - allocs/op",
            "value": 272,
            "unit": "allocs/op",
            "extra": "2012 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create",
            "value": 2529634,
            "unit": "ns/op\t   18296 B/op\t     266 allocs/op",
            "extra": "2150 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create - ns/op",
            "value": 2529634,
            "unit": "ns/op",
            "extra": "2150 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create - B/op",
            "value": 18296,
            "unit": "B/op",
            "extra": "2150 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create - allocs/op",
            "value": 266,
            "unit": "allocs/op",
            "extra": "2150 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_",
            "value": 16461071,
            "unit": "ns/op\t 4464085 B/op\t   45823 allocs/op",
            "extra": "355 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_ - ns/op",
            "value": 16461071,
            "unit": "ns/op",
            "extra": "355 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_ - B/op",
            "value": 4464085,
            "unit": "B/op",
            "extra": "355 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_ - allocs/op",
            "value": 45823,
            "unit": "allocs/op",
            "extra": "355 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_",
            "value": 27862811,
            "unit": "ns/op\t 4508930 B/op\t   45960 allocs/op",
            "extra": "194 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_ - ns/op",
            "value": 27862811,
            "unit": "ns/op",
            "extra": "194 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_ - B/op",
            "value": 4508930,
            "unit": "B/op",
            "extra": "194 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_ - allocs/op",
            "value": 45960,
            "unit": "allocs/op",
            "extra": "194 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_",
            "value": 31617268,
            "unit": "ns/op\t 4440150 B/op\t   46420 allocs/op",
            "extra": "184 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_ - ns/op",
            "value": 31617268,
            "unit": "ns/op",
            "extra": "184 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_ - B/op",
            "value": 4440150,
            "unit": "B/op",
            "extra": "184 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_ - allocs/op",
            "value": 46420,
            "unit": "allocs/op",
            "extra": "184 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_",
            "value": 43890279,
            "unit": "ns/op\t 4653709 B/op\t   46934 allocs/op",
            "extra": "128 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_ - ns/op",
            "value": 43890279,
            "unit": "ns/op",
            "extra": "128 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_ - B/op",
            "value": 4653709,
            "unit": "B/op",
            "extra": "128 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_ - allocs/op",
            "value": 46934,
            "unit": "allocs/op",
            "extra": "128 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_",
            "value": 66269495,
            "unit": "ns/op\t 5170638 B/op\t   48083 allocs/op",
            "extra": "85 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_ - ns/op",
            "value": 66269495,
            "unit": "ns/op",
            "extra": "85 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_ - B/op",
            "value": 5170638,
            "unit": "B/op",
            "extra": "85 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_ - allocs/op",
            "value": 48083,
            "unit": "allocs/op",
            "extra": "85 times"
          },
          {
            "name": "BenchmarkQueryRelationships",
            "value": 436091,
            "unit": "ns/op\t     696 B/op\t      18 allocs/op",
            "extra": "13700 times"
          },
          {
            "name": "BenchmarkQueryRelationships - ns/op",
            "value": 436091,
            "unit": "ns/op",
            "extra": "13700 times"
          },
          {
            "name": "BenchmarkQueryRelationships - B/op",
            "value": 696,
            "unit": "B/op",
            "extra": "13700 times"
          },
          {
            "name": "BenchmarkQueryRelationships - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "13700 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true",
            "value": 2665298,
            "unit": "ns/op\t 2245615 B/op\t   17080 allocs/op",
            "extra": "2288 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true - ns/op",
            "value": 2665298,
            "unit": "ns/op",
            "extra": "2288 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true - B/op",
            "value": 2245615,
            "unit": "B/op",
            "extra": "2288 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true - allocs/op",
            "value": 17080,
            "unit": "allocs/op",
            "extra": "2288 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false",
            "value": 2153995,
            "unit": "ns/op\t 2101433 B/op\t   14076 allocs/op",
            "extra": "2772 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false - ns/op",
            "value": 2153995,
            "unit": "ns/op",
            "extra": "2772 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false - B/op",
            "value": 2101433,
            "unit": "B/op",
            "extra": "2772 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false - allocs/op",
            "value": 14076,
            "unit": "allocs/op",
            "extra": "2772 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash",
            "value": 614.2,
            "unit": "ns/op\t     144 B/op\t       3 allocs/op",
            "extra": "9590568 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash - ns/op",
            "value": 614.2,
            "unit": "ns/op",
            "extra": "9590568 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash - B/op",
            "value": 144,
            "unit": "B/op",
            "extra": "9590568 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9590568 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions",
            "value": 139.9,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "43102821 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions - ns/op",
            "value": 139.9,
            "unit": "ns/op",
            "extra": "43102821 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "43102821 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "43102821 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching",
            "value": 26016,
            "unit": "ns/op\t    3976 B/op\t      72 allocs/op",
            "extra": "319548 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching - ns/op",
            "value": 26016,
            "unit": "ns/op",
            "extra": "319548 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching - B/op",
            "value": 3976,
            "unit": "B/op",
            "extra": "319548 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching - allocs/op",
            "value": 72,
            "unit": "allocs/op",
            "extra": "319548 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression",
            "value": 1868,
            "unit": "ns/op\t    1577 B/op\t      34 allocs/op",
            "extra": "3154167 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression - ns/op",
            "value": 1868,
            "unit": "ns/op",
            "extra": "3154167 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression - B/op",
            "value": 1577,
            "unit": "B/op",
            "extra": "3154167 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression - allocs/op",
            "value": 34,
            "unit": "allocs/op",
            "extra": "3154167 times"
          },
          {
            "name": "BenchmarkPatternMatcher",
            "value": 3239,
            "unit": "ns/op\t    5952 B/op\t      50 allocs/op",
            "extra": "1866082 times"
          },
          {
            "name": "BenchmarkPatternMatcher - ns/op",
            "value": 3239,
            "unit": "ns/op",
            "extra": "1866082 times"
          },
          {
            "name": "BenchmarkPatternMatcher - B/op",
            "value": 5952,
            "unit": "B/op",
            "extra": "1866082 times"
          },
          {
            "name": "BenchmarkPatternMatcher - allocs/op",
            "value": 50,
            "unit": "allocs/op",
            "extra": "1866082 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1",
            "value": 3678867,
            "unit": "ns/op\t  310236 B/op\t    4653 allocs/op",
            "extra": "2230 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1 - ns/op",
            "value": 3678867,
            "unit": "ns/op",
            "extra": "2230 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1 - B/op",
            "value": 310236,
            "unit": "B/op",
            "extra": "2230 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 4653,
            "unit": "allocs/op",
            "extra": "2230 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1",
            "value": 1967999,
            "unit": "ns/op\t  322504 B/op\t    4840 allocs/op",
            "extra": "3619 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1 - ns/op",
            "value": 1967999,
            "unit": "ns/op",
            "extra": "3619 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1 - B/op",
            "value": 322504,
            "unit": "B/op",
            "extra": "3619 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1 - allocs/op",
            "value": 4840,
            "unit": "allocs/op",
            "extra": "3619 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1",
            "value": 811042,
            "unit": "ns/op\t  232813 B/op\t    3593 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1 - ns/op",
            "value": 811042,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1 - B/op",
            "value": 232813,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1 - allocs/op",
            "value": 3593,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1",
            "value": 8434350,
            "unit": "ns/op\t  544871 B/op\t    7538 allocs/op",
            "extra": "850 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1 - ns/op",
            "value": 8434350,
            "unit": "ns/op",
            "extra": "850 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1 - B/op",
            "value": 544871,
            "unit": "B/op",
            "extra": "850 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1 - allocs/op",
            "value": 7538,
            "unit": "allocs/op",
            "extra": "850 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1",
            "value": 4019539,
            "unit": "ns/op\t  586925 B/op\t    8169 allocs/op",
            "extra": "1743 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1 - ns/op",
            "value": 4019539,
            "unit": "ns/op",
            "extra": "1743 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1 - B/op",
            "value": 586925,
            "unit": "B/op",
            "extra": "1743 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1 - allocs/op",
            "value": 8169,
            "unit": "allocs/op",
            "extra": "1743 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1",
            "value": 1051773,
            "unit": "ns/op\t  323237 B/op\t    4492 allocs/op",
            "extra": "9588 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1 - ns/op",
            "value": 1051773,
            "unit": "ns/op",
            "extra": "9588 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1 - B/op",
            "value": 323237,
            "unit": "B/op",
            "extra": "9588 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1 - allocs/op",
            "value": 4492,
            "unit": "allocs/op",
            "extra": "9588 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1",
            "value": 16615501,
            "unit": "ns/op\t 1975366 B/op\t   25453 allocs/op",
            "extra": "462 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1 - ns/op",
            "value": 16615501,
            "unit": "ns/op",
            "extra": "462 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1 - B/op",
            "value": 1975366,
            "unit": "B/op",
            "extra": "462 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1 - allocs/op",
            "value": 25453,
            "unit": "allocs/op",
            "extra": "462 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1",
            "value": 10804947,
            "unit": "ns/op\t 2095497 B/op\t   27008 allocs/op",
            "extra": "748 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1 - ns/op",
            "value": 10804947,
            "unit": "ns/op",
            "extra": "748 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1 - B/op",
            "value": 2095497,
            "unit": "B/op",
            "extra": "748 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1 - allocs/op",
            "value": 27008,
            "unit": "allocs/op",
            "extra": "748 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1",
            "value": 4917745,
            "unit": "ns/op\t 1259407 B/op\t   13549 allocs/op",
            "extra": "2065 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1 - ns/op",
            "value": 4917745,
            "unit": "ns/op",
            "extra": "2065 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1 - B/op",
            "value": 1259407,
            "unit": "B/op",
            "extra": "2065 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1 - allocs/op",
            "value": 13549,
            "unit": "allocs/op",
            "extra": "2065 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1",
            "value": 36567901,
            "unit": "ns/op\t 9933818 B/op\t  136551 allocs/op",
            "extra": "256 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1 - ns/op",
            "value": 36567901,
            "unit": "ns/op",
            "extra": "256 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1 - B/op",
            "value": 9933818,
            "unit": "B/op",
            "extra": "256 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1 - allocs/op",
            "value": 136551,
            "unit": "allocs/op",
            "extra": "256 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1",
            "value": 34340521,
            "unit": "ns/op\t10046511 B/op\t  140157 allocs/op",
            "extra": "176 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1 - ns/op",
            "value": 34340521,
            "unit": "ns/op",
            "extra": "176 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1 - B/op",
            "value": 10046511,
            "unit": "B/op",
            "extra": "176 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1 - allocs/op",
            "value": 140157,
            "unit": "allocs/op",
            "extra": "176 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1",
            "value": 25509157,
            "unit": "ns/op\t 9235803 B/op\t  124942 allocs/op",
            "extra": "237 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1 - ns/op",
            "value": 25509157,
            "unit": "ns/op",
            "extra": "237 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1 - B/op",
            "value": 9235803,
            "unit": "B/op",
            "extra": "237 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1 - allocs/op",
            "value": 124942,
            "unit": "allocs/op",
            "extra": "237 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1",
            "value": 4948875,
            "unit": "ns/op\t  296560 B/op\t    4525 allocs/op",
            "extra": "1698 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1 - ns/op",
            "value": 4948875,
            "unit": "ns/op",
            "extra": "1698 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1 - B/op",
            "value": 296560,
            "unit": "B/op",
            "extra": "1698 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 4525,
            "unit": "allocs/op",
            "extra": "1698 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1",
            "value": 1957424,
            "unit": "ns/op\t  318723 B/op\t    4847 allocs/op",
            "extra": "3495 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1 - ns/op",
            "value": 1957424,
            "unit": "ns/op",
            "extra": "3495 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1 - B/op",
            "value": 318723,
            "unit": "B/op",
            "extra": "3495 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1 - allocs/op",
            "value": 4847,
            "unit": "allocs/op",
            "extra": "3495 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1",
            "value": 472176,
            "unit": "ns/op\t  194558 B/op\t    3194 allocs/op",
            "extra": "12411 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1 - ns/op",
            "value": 472176,
            "unit": "ns/op",
            "extra": "12411 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1 - B/op",
            "value": 194558,
            "unit": "B/op",
            "extra": "12411 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1 - allocs/op",
            "value": 3194,
            "unit": "allocs/op",
            "extra": "12411 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1",
            "value": 9149176,
            "unit": "ns/op\t  946601 B/op\t   14801 allocs/op",
            "extra": "600 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1 - ns/op",
            "value": 9149176,
            "unit": "ns/op",
            "extra": "600 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1 - B/op",
            "value": 946601,
            "unit": "B/op",
            "extra": "600 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 14801,
            "unit": "allocs/op",
            "extra": "600 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1",
            "value": 4476641,
            "unit": "ns/op\t 1021793 B/op\t   16046 allocs/op",
            "extra": "1302 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1 - ns/op",
            "value": 4476641,
            "unit": "ns/op",
            "extra": "1302 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1 - B/op",
            "value": 1021793,
            "unit": "B/op",
            "extra": "1302 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1 - allocs/op",
            "value": 16046,
            "unit": "allocs/op",
            "extra": "1302 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1",
            "value": 1133336,
            "unit": "ns/op\t  636060 B/op\t   11246 allocs/op",
            "extra": "4912 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1 - ns/op",
            "value": 1133336,
            "unit": "ns/op",
            "extra": "4912 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1 - B/op",
            "value": 636060,
            "unit": "B/op",
            "extra": "4912 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1 - allocs/op",
            "value": 11246,
            "unit": "allocs/op",
            "extra": "4912 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1",
            "value": 12715556,
            "unit": "ns/op\t  585119 B/op\t    7856 allocs/op",
            "extra": "644 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1 - ns/op",
            "value": 12715556,
            "unit": "ns/op",
            "extra": "644 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1 - B/op",
            "value": 585119,
            "unit": "B/op",
            "extra": "644 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 7856,
            "unit": "allocs/op",
            "extra": "644 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1",
            "value": 3760251,
            "unit": "ns/op\t  630366 B/op\t    8328 allocs/op",
            "extra": "1543 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1 - ns/op",
            "value": 3760251,
            "unit": "ns/op",
            "extra": "1543 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1 - B/op",
            "value": 630366,
            "unit": "B/op",
            "extra": "1543 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1 - allocs/op",
            "value": 8328,
            "unit": "allocs/op",
            "extra": "1543 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1",
            "value": 687098,
            "unit": "ns/op\t  335012 B/op\t    4094 allocs/op",
            "extra": "7774 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1 - ns/op",
            "value": 687098,
            "unit": "ns/op",
            "extra": "7774 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1 - B/op",
            "value": 335012,
            "unit": "B/op",
            "extra": "7774 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1 - allocs/op",
            "value": 4094,
            "unit": "allocs/op",
            "extra": "7774 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1",
            "value": 3543972,
            "unit": "ns/op\t  134128 B/op\t    2030 allocs/op",
            "extra": "2624 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1 - ns/op",
            "value": 3543972,
            "unit": "ns/op",
            "extra": "2624 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1 - B/op",
            "value": 134128,
            "unit": "B/op",
            "extra": "2624 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1 - allocs/op",
            "value": 2030,
            "unit": "allocs/op",
            "extra": "2624 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1",
            "value": 2475433,
            "unit": "ns/op\t  140645 B/op\t    2106 allocs/op",
            "extra": "2382 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1 - ns/op",
            "value": 2475433,
            "unit": "ns/op",
            "extra": "2382 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1 - B/op",
            "value": 140645,
            "unit": "B/op",
            "extra": "2382 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1 - allocs/op",
            "value": 2106,
            "unit": "allocs/op",
            "extra": "2382 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1",
            "value": 197261,
            "unit": "ns/op\t   82909 B/op\t    1316 allocs/op",
            "extra": "30285 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1 - ns/op",
            "value": 197261,
            "unit": "ns/op",
            "extra": "30285 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1 - B/op",
            "value": 82909,
            "unit": "B/op",
            "extra": "30285 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1 - allocs/op",
            "value": 1316,
            "unit": "allocs/op",
            "extra": "30285 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph",
            "value": 7799,
            "unit": "ns/op\t    6824 B/op\t      98 allocs/op",
            "extra": "765171 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph - ns/op",
            "value": 7799,
            "unit": "ns/op",
            "extra": "765171 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph - B/op",
            "value": 6824,
            "unit": "B/op",
            "extra": "765171 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph - allocs/op",
            "value": 98,
            "unit": "allocs/op",
            "extra": "765171 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph",
            "value": 26657,
            "unit": "ns/op\t   26536 B/op\t     241 allocs/op",
            "extra": "225548 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph - ns/op",
            "value": 26657,
            "unit": "ns/op",
            "extra": "225548 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph - B/op",
            "value": 26536,
            "unit": "B/op",
            "extra": "225548 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph - allocs/op",
            "value": 241,
            "unit": "allocs/op",
            "extra": "225548 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph",
            "value": 25826,
            "unit": "ns/op\t   22952 B/op\t     281 allocs/op",
            "extra": "232489 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph - ns/op",
            "value": 25826,
            "unit": "ns/op",
            "extra": "232489 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph - B/op",
            "value": 22952,
            "unit": "B/op",
            "extra": "232489 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph - allocs/op",
            "value": 281,
            "unit": "allocs/op",
            "extra": "232489 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph",
            "value": 2039,
            "unit": "ns/op\t    1368 B/op\t      32 allocs/op",
            "extra": "2951028 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph - ns/op",
            "value": 2039,
            "unit": "ns/op",
            "extra": "2951028 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph - B/op",
            "value": 1368,
            "unit": "B/op",
            "extra": "2951028 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph - allocs/op",
            "value": 32,
            "unit": "allocs/op",
            "extra": "2951028 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph",
            "value": 32877,
            "unit": "ns/op\t   39505 B/op\t     228 allocs/op",
            "extra": "182683 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph - ns/op",
            "value": 32877,
            "unit": "ns/op",
            "extra": "182683 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph - B/op",
            "value": 39505,
            "unit": "B/op",
            "extra": "182683 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph - allocs/op",
            "value": 228,
            "unit": "allocs/op",
            "extra": "182683 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph",
            "value": 7973,
            "unit": "ns/op\t    6800 B/op\t      99 allocs/op",
            "extra": "748324 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph - ns/op",
            "value": 7973,
            "unit": "ns/op",
            "extra": "748324 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph - B/op",
            "value": 6800,
            "unit": "B/op",
            "extra": "748324 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph - allocs/op",
            "value": 99,
            "unit": "allocs/op",
            "extra": "748324 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources",
            "value": 9336,
            "unit": "ns/op\t    7656 B/op\t     123 allocs/op",
            "extra": "641034 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources - ns/op",
            "value": 9336,
            "unit": "ns/op",
            "extra": "641034 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources - B/op",
            "value": 7656,
            "unit": "B/op",
            "extra": "641034 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources - allocs/op",
            "value": 123,
            "unit": "allocs/op",
            "extra": "641034 times"
          },
          {
            "name": "BenchmarkRelationsReferencing",
            "value": 18.58,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "00000323798253 times"
          },
          {
            "name": "BenchmarkRelationsReferencing - ns/op",
            "value": 18.58,
            "unit": "ns/op",
            "extra": "00000323798253 times"
          },
          {
            "name": "BenchmarkRelationsReferencing - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "00000323798253 times"
          },
          {
            "name": "BenchmarkRelationsReferencing - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "00000323798253 times"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tstirrat@gmail.com",
            "name": "Tanner Stirrat",
            "username": "tstirrat15"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c68435303b00bff14393c4cee40f82c69df6a948",
          "message": "chore: go through other proxies (#2871)",
          "timestamp": "2026-02-02T15:10:25-07:00",
          "tree_id": "5da9d17db6c0c65d2a3bba4f46ff9fc1020f3d2f",
          "url": "https://github.com/authzed/spicedb/commit/c68435303b00bff14393c4cee40f82c69df6a948"
        },
        "date": 1770071001707,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead",
            "value": 369040,
            "unit": "ns/op\t   15342 B/op\t     221 allocs/op",
            "extra": "15796 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead - ns/op",
            "value": 369040,
            "unit": "ns/op",
            "extra": "15796 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead - B/op",
            "value": 15342,
            "unit": "B/op",
            "extra": "15796 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead - allocs/op",
            "value": 221,
            "unit": "allocs/op",
            "extra": "15796 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace",
            "value": 8054175,
            "unit": "ns/op\t   94215 B/op\t   20135 allocs/op",
            "extra": "739 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace - ns/op",
            "value": 8054175,
            "unit": "ns/op",
            "extra": "739 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace - B/op",
            "value": 94215,
            "unit": "B/op",
            "extra": "739 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace - allocs/op",
            "value": 20135,
            "unit": "allocs/op",
            "extra": "739 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource",
            "value": 8025721,
            "unit": "ns/op\t   97356 B/op\t   20194 allocs/op",
            "extra": "744 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - ns/op",
            "value": 8025721,
            "unit": "ns/op",
            "extra": "744 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - B/op",
            "value": 97356,
            "unit": "B/op",
            "extra": "744 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "744 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject",
            "value": 10121035,
            "unit": "ns/op\t   97329 B/op\t   20194 allocs/op",
            "extra": "592 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - ns/op",
            "value": 10121035,
            "unit": "ns/op",
            "extra": "592 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - B/op",
            "value": 97329,
            "unit": "B/op",
            "extra": "592 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "592 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource",
            "value": 7903142,
            "unit": "ns/op\t   77803 B/op\t   15209 allocs/op",
            "extra": "752 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource - ns/op",
            "value": 7903142,
            "unit": "ns/op",
            "extra": "752 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource - B/op",
            "value": 77803,
            "unit": "B/op",
            "extra": "752 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource - allocs/op",
            "value": 15209,
            "unit": "allocs/op",
            "extra": "752 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject",
            "value": 9646316,
            "unit": "ns/op\t   77775 B/op\t   15209 allocs/op",
            "extra": "615 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject - ns/op",
            "value": 9646316,
            "unit": "ns/op",
            "extra": "615 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject - B/op",
            "value": 77775,
            "unit": "B/op",
            "extra": "615 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject - allocs/op",
            "value": 15209,
            "unit": "allocs/op",
            "extra": "615 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource",
            "value": 495817,
            "unit": "ns/op\t   18429 B/op\t     280 allocs/op",
            "extra": "12146 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - ns/op",
            "value": 495817,
            "unit": "ns/op",
            "extra": "12146 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - B/op",
            "value": 18429,
            "unit": "B/op",
            "extra": "12146 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "12146 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject",
            "value": 518155,
            "unit": "ns/op\t   18433 B/op\t     280 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - ns/op",
            "value": 518155,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - B/op",
            "value": 18433,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead",
            "value": 7744719,
            "unit": "ns/op\t  172698 B/op\t   20202 allocs/op",
            "extra": "769 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead - ns/op",
            "value": 7744719,
            "unit": "ns/op",
            "extra": "769 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead - B/op",
            "value": 172698,
            "unit": "B/op",
            "extra": "769 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead - allocs/op",
            "value": 20202,
            "unit": "allocs/op",
            "extra": "769 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch",
            "value": 16326654,
            "unit": "ns/op\t   23443 B/op\t     297 allocs/op",
            "extra": "362 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch - ns/op",
            "value": 16326654,
            "unit": "ns/op",
            "extra": "362 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch - B/op",
            "value": 23443,
            "unit": "B/op",
            "extra": "362 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch - allocs/op",
            "value": 297,
            "unit": "allocs/op",
            "extra": "362 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create",
            "value": 17193830,
            "unit": "ns/op\t   20766 B/op\t     291 allocs/op",
            "extra": "342 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create - ns/op",
            "value": 17193830,
            "unit": "ns/op",
            "extra": "342 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create - B/op",
            "value": 20766,
            "unit": "B/op",
            "extra": "342 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create - allocs/op",
            "value": 291,
            "unit": "allocs/op",
            "extra": "342 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_",
            "value": 24200851,
            "unit": "ns/op\t 4465531 B/op\t   45847 allocs/op",
            "extra": "237 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_ - ns/op",
            "value": 24200851,
            "unit": "ns/op",
            "extra": "237 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_ - B/op",
            "value": 4465531,
            "unit": "B/op",
            "extra": "237 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_ - allocs/op",
            "value": 45847,
            "unit": "allocs/op",
            "extra": "237 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_",
            "value": 41144286,
            "unit": "ns/op\t 4512582 B/op\t   45985 allocs/op",
            "extra": "184 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_ - ns/op",
            "value": 41144286,
            "unit": "ns/op",
            "extra": "184 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_ - B/op",
            "value": 4512582,
            "unit": "B/op",
            "extra": "184 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_ - allocs/op",
            "value": 45985,
            "unit": "allocs/op",
            "extra": "184 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_",
            "value": 33901951,
            "unit": "ns/op\t 4441246 B/op\t   46445 allocs/op",
            "extra": "156 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_ - ns/op",
            "value": 33901951,
            "unit": "ns/op",
            "extra": "156 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_ - B/op",
            "value": 4441246,
            "unit": "B/op",
            "extra": "156 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_ - allocs/op",
            "value": 46445,
            "unit": "allocs/op",
            "extra": "156 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_",
            "value": 44219832,
            "unit": "ns/op\t 4656025 B/op\t   46958 allocs/op",
            "extra": "132 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_ - ns/op",
            "value": 44219832,
            "unit": "ns/op",
            "extra": "132 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_ - B/op",
            "value": 4656025,
            "unit": "B/op",
            "extra": "132 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_ - allocs/op",
            "value": 46958,
            "unit": "allocs/op",
            "extra": "132 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_",
            "value": 65549556,
            "unit": "ns/op\t 5176818 B/op\t   48108 allocs/op",
            "extra": "79 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_ - ns/op",
            "value": 65549556,
            "unit": "ns/op",
            "extra": "79 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_ - B/op",
            "value": 5176818,
            "unit": "B/op",
            "extra": "79 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_ - allocs/op",
            "value": 48108,
            "unit": "allocs/op",
            "extra": "79 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead",
            "value": 365643,
            "unit": "ns/op\t   15341 B/op\t     221 allocs/op",
            "extra": "16449 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead - ns/op",
            "value": 365643,
            "unit": "ns/op",
            "extra": "16449 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead - B/op",
            "value": 15341,
            "unit": "B/op",
            "extra": "16449 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead - allocs/op",
            "value": 221,
            "unit": "allocs/op",
            "extra": "16449 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace",
            "value": 8097181,
            "unit": "ns/op\t   94281 B/op\t   20136 allocs/op",
            "extra": "740 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace - ns/op",
            "value": 8097181,
            "unit": "ns/op",
            "extra": "740 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace - B/op",
            "value": 94281,
            "unit": "B/op",
            "extra": "740 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace - allocs/op",
            "value": 20136,
            "unit": "allocs/op",
            "extra": "740 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource",
            "value": 8117200,
            "unit": "ns/op\t   97340 B/op\t   20194 allocs/op",
            "extra": "739 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - ns/op",
            "value": 8117200,
            "unit": "ns/op",
            "extra": "739 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - B/op",
            "value": 97340,
            "unit": "B/op",
            "extra": "739 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "739 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject",
            "value": 10235673,
            "unit": "ns/op\t   97352 B/op\t   20194 allocs/op",
            "extra": "590 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - ns/op",
            "value": 10235673,
            "unit": "ns/op",
            "extra": "590 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - B/op",
            "value": 97352,
            "unit": "B/op",
            "extra": "590 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "590 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource",
            "value": 7898398,
            "unit": "ns/op\t   77800 B/op\t   15209 allocs/op",
            "extra": "748 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource - ns/op",
            "value": 7898398,
            "unit": "ns/op",
            "extra": "748 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource - B/op",
            "value": 77800,
            "unit": "B/op",
            "extra": "748 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource - allocs/op",
            "value": 15209,
            "unit": "allocs/op",
            "extra": "748 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject",
            "value": 9803042,
            "unit": "ns/op\t   77813 B/op\t   15209 allocs/op",
            "extra": "607 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject - ns/op",
            "value": 9803042,
            "unit": "ns/op",
            "extra": "607 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject - B/op",
            "value": 77813,
            "unit": "B/op",
            "extra": "607 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject - allocs/op",
            "value": 15209,
            "unit": "allocs/op",
            "extra": "607 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource",
            "value": 527178,
            "unit": "ns/op\t   18435 B/op\t     280 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - ns/op",
            "value": 527178,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - B/op",
            "value": 18435,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject",
            "value": 517804,
            "unit": "ns/op\t   18433 B/op\t     280 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - ns/op",
            "value": 517804,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - B/op",
            "value": 18433,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead",
            "value": 7798569,
            "unit": "ns/op\t  172688 B/op\t   20202 allocs/op",
            "extra": "771 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead - ns/op",
            "value": 7798569,
            "unit": "ns/op",
            "extra": "771 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead - B/op",
            "value": 172688,
            "unit": "B/op",
            "extra": "771 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead - allocs/op",
            "value": 20202,
            "unit": "allocs/op",
            "extra": "771 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch",
            "value": 3523140,
            "unit": "ns/op\t   20932 B/op\t     272 allocs/op",
            "extra": "1836 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch - ns/op",
            "value": 3523140,
            "unit": "ns/op",
            "extra": "1836 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch - B/op",
            "value": 20932,
            "unit": "B/op",
            "extra": "1836 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch - allocs/op",
            "value": 272,
            "unit": "allocs/op",
            "extra": "1836 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create",
            "value": 1988292,
            "unit": "ns/op\t   18285 B/op\t     266 allocs/op",
            "extra": "3042 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create - ns/op",
            "value": 1988292,
            "unit": "ns/op",
            "extra": "3042 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create - B/op",
            "value": 18285,
            "unit": "B/op",
            "extra": "3042 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create - allocs/op",
            "value": 266,
            "unit": "allocs/op",
            "extra": "3042 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_",
            "value": 16371795,
            "unit": "ns/op\t 4463225 B/op\t   45822 allocs/op",
            "extra": "357 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_ - ns/op",
            "value": 16371795,
            "unit": "ns/op",
            "extra": "357 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_ - B/op",
            "value": 4463225,
            "unit": "B/op",
            "extra": "357 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_ - allocs/op",
            "value": 45822,
            "unit": "allocs/op",
            "extra": "357 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_",
            "value": 25897885,
            "unit": "ns/op\t 4508774 B/op\t   45959 allocs/op",
            "extra": "249 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_ - ns/op",
            "value": 25897885,
            "unit": "ns/op",
            "extra": "249 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_ - B/op",
            "value": 4508774,
            "unit": "B/op",
            "extra": "249 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_ - allocs/op",
            "value": 45959,
            "unit": "allocs/op",
            "extra": "249 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_",
            "value": 30039934,
            "unit": "ns/op\t 4440232 B/op\t   46420 allocs/op",
            "extra": "174 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_ - ns/op",
            "value": 30039934,
            "unit": "ns/op",
            "extra": "174 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_ - B/op",
            "value": 4440232,
            "unit": "B/op",
            "extra": "174 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_ - allocs/op",
            "value": 46420,
            "unit": "allocs/op",
            "extra": "174 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_",
            "value": 43138454,
            "unit": "ns/op\t 4653403 B/op\t   46931 allocs/op",
            "extra": "130 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_ - ns/op",
            "value": 43138454,
            "unit": "ns/op",
            "extra": "130 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_ - B/op",
            "value": 4653403,
            "unit": "B/op",
            "extra": "130 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_ - allocs/op",
            "value": 46931,
            "unit": "allocs/op",
            "extra": "130 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_",
            "value": 64046273,
            "unit": "ns/op\t 5170828 B/op\t   48086 allocs/op",
            "extra": "88 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_ - ns/op",
            "value": 64046273,
            "unit": "ns/op",
            "extra": "88 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_ - B/op",
            "value": 5170828,
            "unit": "B/op",
            "extra": "88 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_ - allocs/op",
            "value": 48086,
            "unit": "allocs/op",
            "extra": "88 times"
          },
          {
            "name": "BenchmarkQueryRelationships",
            "value": 447489,
            "unit": "ns/op\t     696 B/op\t      18 allocs/op",
            "extra": "13444 times"
          },
          {
            "name": "BenchmarkQueryRelationships - ns/op",
            "value": 447489,
            "unit": "ns/op",
            "extra": "13444 times"
          },
          {
            "name": "BenchmarkQueryRelationships - B/op",
            "value": 696,
            "unit": "B/op",
            "extra": "13444 times"
          },
          {
            "name": "BenchmarkQueryRelationships - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "13444 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true",
            "value": 2780274,
            "unit": "ns/op\t 2245615 B/op\t   17080 allocs/op",
            "extra": "2252 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true - ns/op",
            "value": 2780274,
            "unit": "ns/op",
            "extra": "2252 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true - B/op",
            "value": 2245615,
            "unit": "B/op",
            "extra": "2252 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true - allocs/op",
            "value": 17080,
            "unit": "allocs/op",
            "extra": "2252 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false",
            "value": 2312327,
            "unit": "ns/op\t 2101432 B/op\t   14076 allocs/op",
            "extra": "2674 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false - ns/op",
            "value": 2312327,
            "unit": "ns/op",
            "extra": "2674 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false - B/op",
            "value": 2101432,
            "unit": "B/op",
            "extra": "2674 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false - allocs/op",
            "value": 14076,
            "unit": "allocs/op",
            "extra": "2674 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash",
            "value": 614.8,
            "unit": "ns/op\t     144 B/op\t       3 allocs/op",
            "extra": "9687643 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash - ns/op",
            "value": 614.8,
            "unit": "ns/op",
            "extra": "9687643 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash - B/op",
            "value": 144,
            "unit": "B/op",
            "extra": "9687643 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9687643 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions",
            "value": 139.1,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "43002003 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions - ns/op",
            "value": 139.1,
            "unit": "ns/op",
            "extra": "43002003 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "43002003 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "43002003 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching",
            "value": 26307,
            "unit": "ns/op\t    3977 B/op\t      72 allocs/op",
            "extra": "311260 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching - ns/op",
            "value": 26307,
            "unit": "ns/op",
            "extra": "311260 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching - B/op",
            "value": 3977,
            "unit": "B/op",
            "extra": "311260 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching - allocs/op",
            "value": 72,
            "unit": "allocs/op",
            "extra": "311260 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression",
            "value": 2028,
            "unit": "ns/op\t    1577 B/op\t      34 allocs/op",
            "extra": "2970873 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression - ns/op",
            "value": 2028,
            "unit": "ns/op",
            "extra": "2970873 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression - B/op",
            "value": 1577,
            "unit": "B/op",
            "extra": "2970873 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression - allocs/op",
            "value": 34,
            "unit": "allocs/op",
            "extra": "2970873 times"
          },
          {
            "name": "BenchmarkPatternMatcher",
            "value": 3545,
            "unit": "ns/op\t    5952 B/op\t      50 allocs/op",
            "extra": "1701604 times"
          },
          {
            "name": "BenchmarkPatternMatcher - ns/op",
            "value": 3545,
            "unit": "ns/op",
            "extra": "1701604 times"
          },
          {
            "name": "BenchmarkPatternMatcher - B/op",
            "value": 5952,
            "unit": "B/op",
            "extra": "1701604 times"
          },
          {
            "name": "BenchmarkPatternMatcher - allocs/op",
            "value": 50,
            "unit": "allocs/op",
            "extra": "1701604 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1",
            "value": 3914593,
            "unit": "ns/op\t  314081 B/op\t    4745 allocs/op",
            "extra": "2058 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1 - ns/op",
            "value": 3914593,
            "unit": "ns/op",
            "extra": "2058 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1 - B/op",
            "value": 314081,
            "unit": "B/op",
            "extra": "2058 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 4745,
            "unit": "allocs/op",
            "extra": "2058 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1",
            "value": 2087695,
            "unit": "ns/op\t  326051 B/op\t    4946 allocs/op",
            "extra": "3416 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1 - ns/op",
            "value": 2087695,
            "unit": "ns/op",
            "extra": "3416 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1 - B/op",
            "value": 326051,
            "unit": "B/op",
            "extra": "3416 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1 - allocs/op",
            "value": 4946,
            "unit": "allocs/op",
            "extra": "3416 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1",
            "value": 853017,
            "unit": "ns/op\t  236449 B/op\t    3677 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1 - ns/op",
            "value": 853017,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1 - B/op",
            "value": 236449,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1 - allocs/op",
            "value": 3677,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1",
            "value": 8982584,
            "unit": "ns/op\t  552742 B/op\t    7733 allocs/op",
            "extra": "820 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1 - ns/op",
            "value": 8982584,
            "unit": "ns/op",
            "extra": "820 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1 - B/op",
            "value": 552742,
            "unit": "B/op",
            "extra": "820 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1 - allocs/op",
            "value": 7733,
            "unit": "allocs/op",
            "extra": "820 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1",
            "value": 4415951,
            "unit": "ns/op\t  593364 B/op\t    8345 allocs/op",
            "extra": "1604 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1 - ns/op",
            "value": 4415951,
            "unit": "ns/op",
            "extra": "1604 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1 - B/op",
            "value": 593364,
            "unit": "B/op",
            "extra": "1604 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1 - allocs/op",
            "value": 8345,
            "unit": "allocs/op",
            "extra": "1604 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1",
            "value": 1257339,
            "unit": "ns/op\t  329699 B/op\t    4649 allocs/op",
            "extra": "7658 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1 - ns/op",
            "value": 1257339,
            "unit": "ns/op",
            "extra": "7658 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1 - B/op",
            "value": 329699,
            "unit": "B/op",
            "extra": "7658 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1 - allocs/op",
            "value": 4649,
            "unit": "allocs/op",
            "extra": "7658 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1",
            "value": 17241847,
            "unit": "ns/op\t 1988008 B/op\t   25785 allocs/op",
            "extra": "418 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1 - ns/op",
            "value": 17241847,
            "unit": "ns/op",
            "extra": "418 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1 - B/op",
            "value": 1988008,
            "unit": "B/op",
            "extra": "418 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1 - allocs/op",
            "value": 25785,
            "unit": "allocs/op",
            "extra": "418 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1",
            "value": 11712850,
            "unit": "ns/op\t 2110471 B/op\t   27401 allocs/op",
            "extra": "718 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1 - ns/op",
            "value": 11712850,
            "unit": "ns/op",
            "extra": "718 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1 - B/op",
            "value": 2110471,
            "unit": "B/op",
            "extra": "718 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1 - allocs/op",
            "value": 27401,
            "unit": "allocs/op",
            "extra": "718 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1",
            "value": 5575303,
            "unit": "ns/op\t 1274386 B/op\t   13900 allocs/op",
            "extra": "1970 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1 - ns/op",
            "value": 5575303,
            "unit": "ns/op",
            "extra": "1970 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1 - B/op",
            "value": 1274386,
            "unit": "B/op",
            "extra": "1970 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1 - allocs/op",
            "value": 13900,
            "unit": "allocs/op",
            "extra": "1970 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1",
            "value": 41547022,
            "unit": "ns/op\t 9968680 B/op\t  137458 allocs/op",
            "extra": "249 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1 - ns/op",
            "value": 41547022,
            "unit": "ns/op",
            "extra": "249 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1 - B/op",
            "value": 9968680,
            "unit": "B/op",
            "extra": "249 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1 - allocs/op",
            "value": 137458,
            "unit": "allocs/op",
            "extra": "249 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1",
            "value": 36200664,
            "unit": "ns/op\t10084635 B/op\t  141087 allocs/op",
            "extra": "164 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1 - ns/op",
            "value": 36200664,
            "unit": "ns/op",
            "extra": "164 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1 - B/op",
            "value": 10084635,
            "unit": "B/op",
            "extra": "164 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1 - allocs/op",
            "value": 141087,
            "unit": "allocs/op",
            "extra": "164 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1",
            "value": 29373635,
            "unit": "ns/op\t 9277155 B/op\t  125892 allocs/op",
            "extra": "199 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1 - ns/op",
            "value": 29373635,
            "unit": "ns/op",
            "extra": "199 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1 - B/op",
            "value": 9277155,
            "unit": "B/op",
            "extra": "199 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1 - allocs/op",
            "value": 125892,
            "unit": "allocs/op",
            "extra": "199 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1",
            "value": 5285558,
            "unit": "ns/op\t  303124 B/op\t    4684 allocs/op",
            "extra": "1572 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1 - ns/op",
            "value": 5285558,
            "unit": "ns/op",
            "extra": "1572 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1 - B/op",
            "value": 303124,
            "unit": "B/op",
            "extra": "1572 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 4684,
            "unit": "allocs/op",
            "extra": "1572 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1",
            "value": 2067161,
            "unit": "ns/op\t  325028 B/op\t    5021 allocs/op",
            "extra": "3344 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1 - ns/op",
            "value": 2067161,
            "unit": "ns/op",
            "extra": "3344 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1 - B/op",
            "value": 325028,
            "unit": "B/op",
            "extra": "3344 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1 - allocs/op",
            "value": 5021,
            "unit": "allocs/op",
            "extra": "3344 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1",
            "value": 528567,
            "unit": "ns/op\t  200795 B/op\t    3340 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1 - ns/op",
            "value": 528567,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1 - B/op",
            "value": 200795,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1 - allocs/op",
            "value": 3340,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1",
            "value": 11302123,
            "unit": "ns/op\t  970323 B/op\t   15376 allocs/op",
            "extra": "625 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1 - ns/op",
            "value": 11302123,
            "unit": "ns/op",
            "extra": "625 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1 - B/op",
            "value": 970323,
            "unit": "B/op",
            "extra": "625 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 15376,
            "unit": "allocs/op",
            "extra": "625 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1",
            "value": 4757375,
            "unit": "ns/op\t 1038380 B/op\t   16485 allocs/op",
            "extra": "1220 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1 - ns/op",
            "value": 4757375,
            "unit": "ns/op",
            "extra": "1220 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1 - B/op",
            "value": 1038380,
            "unit": "B/op",
            "extra": "1220 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1 - allocs/op",
            "value": 16485,
            "unit": "allocs/op",
            "extra": "1220 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1",
            "value": 1251514,
            "unit": "ns/op\t  652558 B/op\t   11633 allocs/op",
            "extra": "4432 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1 - ns/op",
            "value": 1251514,
            "unit": "ns/op",
            "extra": "4432 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1 - B/op",
            "value": 652558,
            "unit": "B/op",
            "extra": "4432 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1 - allocs/op",
            "value": 11633,
            "unit": "allocs/op",
            "extra": "4432 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1",
            "value": 11498633,
            "unit": "ns/op\t  586760 B/op\t    7947 allocs/op",
            "extra": "541 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1 - ns/op",
            "value": 11498633,
            "unit": "ns/op",
            "extra": "541 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1 - B/op",
            "value": 586760,
            "unit": "B/op",
            "extra": "541 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 7947,
            "unit": "allocs/op",
            "extra": "541 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1",
            "value": 3965959,
            "unit": "ns/op\t  638044 B/op\t    8539 allocs/op",
            "extra": "1488 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1 - ns/op",
            "value": 3965959,
            "unit": "ns/op",
            "extra": "1488 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1 - B/op",
            "value": 638044,
            "unit": "B/op",
            "extra": "1488 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1 - allocs/op",
            "value": 8539,
            "unit": "allocs/op",
            "extra": "1488 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1",
            "value": 746932,
            "unit": "ns/op\t  342538 B/op\t    4272 allocs/op",
            "extra": "7486 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1 - ns/op",
            "value": 746932,
            "unit": "ns/op",
            "extra": "7486 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1 - B/op",
            "value": 342538,
            "unit": "B/op",
            "extra": "7486 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1 - allocs/op",
            "value": 4272,
            "unit": "allocs/op",
            "extra": "7486 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1",
            "value": 4015566,
            "unit": "ns/op\t  137772 B/op\t    2120 allocs/op",
            "extra": "2373 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1 - ns/op",
            "value": 4015566,
            "unit": "ns/op",
            "extra": "2373 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1 - B/op",
            "value": 137772,
            "unit": "B/op",
            "extra": "2373 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1 - allocs/op",
            "value": 2120,
            "unit": "allocs/op",
            "extra": "2373 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1",
            "value": 2609118,
            "unit": "ns/op\t  144088 B/op\t    2209 allocs/op",
            "extra": "2264 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1 - ns/op",
            "value": 2609118,
            "unit": "ns/op",
            "extra": "2264 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1 - B/op",
            "value": 144088,
            "unit": "B/op",
            "extra": "2264 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1 - allocs/op",
            "value": 2209,
            "unit": "allocs/op",
            "extra": "2264 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1",
            "value": 219430,
            "unit": "ns/op\t   86190 B/op\t    1395 allocs/op",
            "extra": "27666 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1 - ns/op",
            "value": 219430,
            "unit": "ns/op",
            "extra": "27666 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1 - B/op",
            "value": 86190,
            "unit": "B/op",
            "extra": "27666 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1 - allocs/op",
            "value": 1395,
            "unit": "allocs/op",
            "extra": "27666 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph",
            "value": 8383,
            "unit": "ns/op\t    6824 B/op\t      98 allocs/op",
            "extra": "721834 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph - ns/op",
            "value": 8383,
            "unit": "ns/op",
            "extra": "721834 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph - B/op",
            "value": 6824,
            "unit": "B/op",
            "extra": "721834 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph - allocs/op",
            "value": 98,
            "unit": "allocs/op",
            "extra": "721834 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph",
            "value": 29225,
            "unit": "ns/op\t   26536 B/op\t     241 allocs/op",
            "extra": "203960 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph - ns/op",
            "value": 29225,
            "unit": "ns/op",
            "extra": "203960 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph - B/op",
            "value": 26536,
            "unit": "B/op",
            "extra": "203960 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph - allocs/op",
            "value": 241,
            "unit": "allocs/op",
            "extra": "203960 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph",
            "value": 27766,
            "unit": "ns/op\t   22952 B/op\t     281 allocs/op",
            "extra": "215806 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph - ns/op",
            "value": 27766,
            "unit": "ns/op",
            "extra": "215806 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph - B/op",
            "value": 22952,
            "unit": "B/op",
            "extra": "215806 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph - allocs/op",
            "value": 281,
            "unit": "allocs/op",
            "extra": "215806 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph",
            "value": 2157,
            "unit": "ns/op\t    1368 B/op\t      32 allocs/op",
            "extra": "2778199 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph - ns/op",
            "value": 2157,
            "unit": "ns/op",
            "extra": "2778199 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph - B/op",
            "value": 1368,
            "unit": "B/op",
            "extra": "2778199 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph - allocs/op",
            "value": 32,
            "unit": "allocs/op",
            "extra": "2778199 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph",
            "value": 35694,
            "unit": "ns/op\t   39505 B/op\t     228 allocs/op",
            "extra": "169357 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph - ns/op",
            "value": 35694,
            "unit": "ns/op",
            "extra": "169357 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph - B/op",
            "value": 39505,
            "unit": "B/op",
            "extra": "169357 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph - allocs/op",
            "value": 228,
            "unit": "allocs/op",
            "extra": "169357 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph",
            "value": 8565,
            "unit": "ns/op\t    6800 B/op\t      99 allocs/op",
            "extra": "703542 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph - ns/op",
            "value": 8565,
            "unit": "ns/op",
            "extra": "703542 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph - B/op",
            "value": 6800,
            "unit": "B/op",
            "extra": "703542 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph - allocs/op",
            "value": 99,
            "unit": "allocs/op",
            "extra": "703542 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources",
            "value": 9907,
            "unit": "ns/op\t    7656 B/op\t     123 allocs/op",
            "extra": "606724 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources - ns/op",
            "value": 9907,
            "unit": "ns/op",
            "extra": "606724 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources - B/op",
            "value": 7656,
            "unit": "B/op",
            "extra": "606724 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources - allocs/op",
            "value": 123,
            "unit": "allocs/op",
            "extra": "606724 times"
          },
          {
            "name": "BenchmarkRelationsReferencing",
            "value": 18.55,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "00000323826187 times"
          },
          {
            "name": "BenchmarkRelationsReferencing - ns/op",
            "value": 18.55,
            "unit": "ns/op",
            "extra": "00000323826187 times"
          },
          {
            "name": "BenchmarkRelationsReferencing - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "00000323826187 times"
          },
          {
            "name": "BenchmarkRelationsReferencing - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "00000323826187 times"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tstirrat@gmail.com",
            "name": "Tanner Stirrat",
            "username": "tstirrat15"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "10cc7f72eca3b47bb911bf1f50c701c1dc7d47e1",
          "message": "fix: hook up schema caching proxies with schemareader (#2868)\n\nCo-authored-by: Maria Ines Parnisari <maria.ines.parnisari@authzed.com>",
          "timestamp": "2026-02-03T09:10:33-07:00",
          "tree_id": "1e5a27daee86ecf620a3048f6fbf9d661c9748d4",
          "url": "https://github.com/authzed/spicedb/commit/10cc7f72eca3b47bb911bf1f50c701c1dc7d47e1"
        },
        "date": 1770136329672,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead",
            "value": 393390,
            "unit": "ns/op\t   15342 B/op\t     221 allocs/op",
            "extra": "15019 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead - ns/op",
            "value": 393390,
            "unit": "ns/op",
            "extra": "15019 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead - B/op",
            "value": 15342,
            "unit": "B/op",
            "extra": "15019 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead - allocs/op",
            "value": 221,
            "unit": "allocs/op",
            "extra": "15019 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace",
            "value": 8186935,
            "unit": "ns/op\t   94255 B/op\t   20135 allocs/op",
            "extra": "730 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace - ns/op",
            "value": 8186935,
            "unit": "ns/op",
            "extra": "730 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace - B/op",
            "value": 94255,
            "unit": "B/op",
            "extra": "730 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace - allocs/op",
            "value": 20135,
            "unit": "allocs/op",
            "extra": "730 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource",
            "value": 8348451,
            "unit": "ns/op\t   97299 B/op\t   20194 allocs/op",
            "extra": "715 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - ns/op",
            "value": 8348451,
            "unit": "ns/op",
            "extra": "715 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - B/op",
            "value": 97299,
            "unit": "B/op",
            "extra": "715 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "715 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject",
            "value": 10311388,
            "unit": "ns/op\t   97324 B/op\t   20194 allocs/op",
            "extra": "571 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - ns/op",
            "value": 10311388,
            "unit": "ns/op",
            "extra": "571 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - B/op",
            "value": 97324,
            "unit": "B/op",
            "extra": "571 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "571 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource",
            "value": 8066491,
            "unit": "ns/op\t   77797 B/op\t   15209 allocs/op",
            "extra": "750 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource - ns/op",
            "value": 8066491,
            "unit": "ns/op",
            "extra": "750 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource - B/op",
            "value": 77797,
            "unit": "B/op",
            "extra": "750 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource - allocs/op",
            "value": 15209,
            "unit": "allocs/op",
            "extra": "750 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject",
            "value": 10095735,
            "unit": "ns/op\t   77806 B/op\t   15209 allocs/op",
            "extra": "591 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject - ns/op",
            "value": 10095735,
            "unit": "ns/op",
            "extra": "591 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject - B/op",
            "value": 77806,
            "unit": "B/op",
            "extra": "591 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject - allocs/op",
            "value": 15209,
            "unit": "allocs/op",
            "extra": "591 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource",
            "value": 522766,
            "unit": "ns/op\t   18428 B/op\t     280 allocs/op",
            "extra": "12770 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - ns/op",
            "value": 522766,
            "unit": "ns/op",
            "extra": "12770 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - B/op",
            "value": 18428,
            "unit": "B/op",
            "extra": "12770 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "12770 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject",
            "value": 540965,
            "unit": "ns/op\t   18433 B/op\t     280 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - ns/op",
            "value": 540965,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - B/op",
            "value": 18433,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead",
            "value": 7883583,
            "unit": "ns/op\t  172773 B/op\t   20203 allocs/op",
            "extra": "753 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead - ns/op",
            "value": 7883583,
            "unit": "ns/op",
            "extra": "753 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead - B/op",
            "value": 172773,
            "unit": "B/op",
            "extra": "753 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead - allocs/op",
            "value": 20203,
            "unit": "allocs/op",
            "extra": "753 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch",
            "value": 16383601,
            "unit": "ns/op\t   23392 B/op\t     297 allocs/op",
            "extra": "369 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch - ns/op",
            "value": 16383601,
            "unit": "ns/op",
            "extra": "369 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch - B/op",
            "value": 23392,
            "unit": "B/op",
            "extra": "369 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch - allocs/op",
            "value": 297,
            "unit": "allocs/op",
            "extra": "369 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create",
            "value": 17153082,
            "unit": "ns/op\t   20761 B/op\t     291 allocs/op",
            "extra": "357 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create - ns/op",
            "value": 17153082,
            "unit": "ns/op",
            "extra": "357 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create - B/op",
            "value": 20761,
            "unit": "B/op",
            "extra": "357 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create - allocs/op",
            "value": 291,
            "unit": "allocs/op",
            "extra": "357 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_",
            "value": 24688750,
            "unit": "ns/op\t 4466223 B/op\t   45847 allocs/op",
            "extra": "240 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_ - ns/op",
            "value": 24688750,
            "unit": "ns/op",
            "extra": "240 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_ - B/op",
            "value": 4466223,
            "unit": "B/op",
            "extra": "240 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_ - allocs/op",
            "value": 45847,
            "unit": "allocs/op",
            "extra": "240 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_",
            "value": 35096076,
            "unit": "ns/op\t 4511185 B/op\t   45984 allocs/op",
            "extra": "177 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_ - ns/op",
            "value": 35096076,
            "unit": "ns/op",
            "extra": "177 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_ - B/op",
            "value": 4511185,
            "unit": "B/op",
            "extra": "177 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_ - allocs/op",
            "value": 45984,
            "unit": "allocs/op",
            "extra": "177 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_",
            "value": 37657412,
            "unit": "ns/op\t 4443174 B/op\t   46445 allocs/op",
            "extra": "135 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_ - ns/op",
            "value": 37657412,
            "unit": "ns/op",
            "extra": "135 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_ - B/op",
            "value": 4443174,
            "unit": "B/op",
            "extra": "135 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_ - allocs/op",
            "value": 46445,
            "unit": "allocs/op",
            "extra": "135 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_",
            "value": 46307205,
            "unit": "ns/op\t 4656045 B/op\t   46958 allocs/op",
            "extra": "127 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_ - ns/op",
            "value": 46307205,
            "unit": "ns/op",
            "extra": "127 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_ - B/op",
            "value": 4656045,
            "unit": "B/op",
            "extra": "127 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_ - allocs/op",
            "value": 46958,
            "unit": "allocs/op",
            "extra": "127 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_",
            "value": 68480848,
            "unit": "ns/op\t 5173005 B/op\t   48110 allocs/op",
            "extra": "90 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_ - ns/op",
            "value": 68480848,
            "unit": "ns/op",
            "extra": "90 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_ - B/op",
            "value": 5173005,
            "unit": "B/op",
            "extra": "90 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_ - allocs/op",
            "value": 48110,
            "unit": "allocs/op",
            "extra": "90 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead",
            "value": 394812,
            "unit": "ns/op\t   15341 B/op\t     221 allocs/op",
            "extra": "15127 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead - ns/op",
            "value": 394812,
            "unit": "ns/op",
            "extra": "15127 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead - B/op",
            "value": 15341,
            "unit": "B/op",
            "extra": "15127 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead - allocs/op",
            "value": 221,
            "unit": "allocs/op",
            "extra": "15127 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace",
            "value": 8327466,
            "unit": "ns/op\t   94326 B/op\t   20136 allocs/op",
            "extra": "716 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace - ns/op",
            "value": 8327466,
            "unit": "ns/op",
            "extra": "716 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace - B/op",
            "value": 94326,
            "unit": "B/op",
            "extra": "716 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace - allocs/op",
            "value": 20136,
            "unit": "allocs/op",
            "extra": "716 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource",
            "value": 8338884,
            "unit": "ns/op\t   97345 B/op\t   20194 allocs/op",
            "extra": "723 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - ns/op",
            "value": 8338884,
            "unit": "ns/op",
            "extra": "723 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - B/op",
            "value": 97345,
            "unit": "B/op",
            "extra": "723 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "723 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject",
            "value": 10308844,
            "unit": "ns/op\t   97378 B/op\t   20195 allocs/op",
            "extra": "578 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - ns/op",
            "value": 10308844,
            "unit": "ns/op",
            "extra": "578 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - B/op",
            "value": 97378,
            "unit": "B/op",
            "extra": "578 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - allocs/op",
            "value": 20195,
            "unit": "allocs/op",
            "extra": "578 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource",
            "value": 8086720,
            "unit": "ns/op\t   77769 B/op\t   15209 allocs/op",
            "extra": "748 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource - ns/op",
            "value": 8086720,
            "unit": "ns/op",
            "extra": "748 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource - B/op",
            "value": 77769,
            "unit": "B/op",
            "extra": "748 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource - allocs/op",
            "value": 15209,
            "unit": "allocs/op",
            "extra": "748 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject",
            "value": 10079614,
            "unit": "ns/op\t   77807 B/op\t   15209 allocs/op",
            "extra": "594 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject - ns/op",
            "value": 10079614,
            "unit": "ns/op",
            "extra": "594 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject - B/op",
            "value": 77807,
            "unit": "B/op",
            "extra": "594 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject - allocs/op",
            "value": 15209,
            "unit": "allocs/op",
            "extra": "594 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource",
            "value": 542755,
            "unit": "ns/op\t   18434 B/op\t     280 allocs/op",
            "extra": "14128 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - ns/op",
            "value": 542755,
            "unit": "ns/op",
            "extra": "14128 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - B/op",
            "value": 18434,
            "unit": "B/op",
            "extra": "14128 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "14128 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject",
            "value": 543675,
            "unit": "ns/op\t   18431 B/op\t     280 allocs/op",
            "extra": "9795 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - ns/op",
            "value": 543675,
            "unit": "ns/op",
            "extra": "9795 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - B/op",
            "value": 18431,
            "unit": "B/op",
            "extra": "9795 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "9795 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead",
            "value": 7978662,
            "unit": "ns/op\t  172694 B/op\t   20202 allocs/op",
            "extra": "734 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead - ns/op",
            "value": 7978662,
            "unit": "ns/op",
            "extra": "734 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead - B/op",
            "value": 172694,
            "unit": "B/op",
            "extra": "734 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead - allocs/op",
            "value": 20202,
            "unit": "allocs/op",
            "extra": "734 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch",
            "value": 3676491,
            "unit": "ns/op\t   20941 B/op\t     272 allocs/op",
            "extra": "1566 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch - ns/op",
            "value": 3676491,
            "unit": "ns/op",
            "extra": "1566 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch - B/op",
            "value": 20941,
            "unit": "B/op",
            "extra": "1566 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch - allocs/op",
            "value": 272,
            "unit": "allocs/op",
            "extra": "1566 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create",
            "value": 1693834,
            "unit": "ns/op\t   18305 B/op\t     266 allocs/op",
            "extra": "3506 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create - ns/op",
            "value": 1693834,
            "unit": "ns/op",
            "extra": "3506 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create - B/op",
            "value": 18305,
            "unit": "B/op",
            "extra": "3506 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create - allocs/op",
            "value": 266,
            "unit": "allocs/op",
            "extra": "3506 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_",
            "value": 17147686,
            "unit": "ns/op\t 4462160 B/op\t   45823 allocs/op",
            "extra": "339 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_ - ns/op",
            "value": 17147686,
            "unit": "ns/op",
            "extra": "339 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_ - B/op",
            "value": 4462160,
            "unit": "B/op",
            "extra": "339 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_ - allocs/op",
            "value": 45823,
            "unit": "allocs/op",
            "extra": "339 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_",
            "value": 35728519,
            "unit": "ns/op\t 4508911 B/op\t   45960 allocs/op",
            "extra": "214 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_ - ns/op",
            "value": 35728519,
            "unit": "ns/op",
            "extra": "214 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_ - B/op",
            "value": 4508911,
            "unit": "B/op",
            "extra": "214 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_ - allocs/op",
            "value": 45960,
            "unit": "allocs/op",
            "extra": "214 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_",
            "value": 32859978,
            "unit": "ns/op\t 4440178 B/op\t   46419 allocs/op",
            "extra": "168 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_ - ns/op",
            "value": 32859978,
            "unit": "ns/op",
            "extra": "168 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_ - B/op",
            "value": 4440178,
            "unit": "B/op",
            "extra": "168 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_ - allocs/op",
            "value": 46419,
            "unit": "allocs/op",
            "extra": "168 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_",
            "value": 48926685,
            "unit": "ns/op\t 4653757 B/op\t   46934 allocs/op",
            "extra": "121 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_ - ns/op",
            "value": 48926685,
            "unit": "ns/op",
            "extra": "121 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_ - B/op",
            "value": 4653757,
            "unit": "B/op",
            "extra": "121 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_ - allocs/op",
            "value": 46934,
            "unit": "allocs/op",
            "extra": "121 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_",
            "value": 69868647,
            "unit": "ns/op\t 5171138 B/op\t   48086 allocs/op",
            "extra": "84 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_ - ns/op",
            "value": 69868647,
            "unit": "ns/op",
            "extra": "84 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_ - B/op",
            "value": 5171138,
            "unit": "B/op",
            "extra": "84 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_ - allocs/op",
            "value": 48086,
            "unit": "allocs/op",
            "extra": "84 times"
          },
          {
            "name": "BenchmarkQueryRelationships",
            "value": 440144,
            "unit": "ns/op\t     696 B/op\t      18 allocs/op",
            "extra": "13581 times"
          },
          {
            "name": "BenchmarkQueryRelationships - ns/op",
            "value": 440144,
            "unit": "ns/op",
            "extra": "13581 times"
          },
          {
            "name": "BenchmarkQueryRelationships - B/op",
            "value": 696,
            "unit": "B/op",
            "extra": "13581 times"
          },
          {
            "name": "BenchmarkQueryRelationships - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "13581 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true",
            "value": 3246935,
            "unit": "ns/op\t 2245616 B/op\t   17080 allocs/op",
            "extra": "1940 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true - ns/op",
            "value": 3246935,
            "unit": "ns/op",
            "extra": "1940 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true - B/op",
            "value": 2245616,
            "unit": "B/op",
            "extra": "1940 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true - allocs/op",
            "value": 17080,
            "unit": "allocs/op",
            "extra": "1940 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false",
            "value": 2650179,
            "unit": "ns/op\t 2101433 B/op\t   14076 allocs/op",
            "extra": "2299 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false - ns/op",
            "value": 2650179,
            "unit": "ns/op",
            "extra": "2299 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false - B/op",
            "value": 2101433,
            "unit": "B/op",
            "extra": "2299 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false - allocs/op",
            "value": 14076,
            "unit": "allocs/op",
            "extra": "2299 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash",
            "value": 623.2,
            "unit": "ns/op\t     144 B/op\t       3 allocs/op",
            "extra": "9611130 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash - ns/op",
            "value": 623.2,
            "unit": "ns/op",
            "extra": "9611130 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash - B/op",
            "value": 144,
            "unit": "B/op",
            "extra": "9611130 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9611130 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions",
            "value": 140.8,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "42823110 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions - ns/op",
            "value": 140.8,
            "unit": "ns/op",
            "extra": "42823110 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "42823110 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "42823110 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching",
            "value": 27447,
            "unit": "ns/op\t    3980 B/op\t      72 allocs/op",
            "extra": "299808 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching - ns/op",
            "value": 27447,
            "unit": "ns/op",
            "extra": "299808 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching - B/op",
            "value": 3980,
            "unit": "B/op",
            "extra": "299808 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching - allocs/op",
            "value": 72,
            "unit": "allocs/op",
            "extra": "299808 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression",
            "value": 2150,
            "unit": "ns/op\t    1577 B/op\t      34 allocs/op",
            "extra": "2759596 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression - ns/op",
            "value": 2150,
            "unit": "ns/op",
            "extra": "2759596 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression - B/op",
            "value": 1577,
            "unit": "B/op",
            "extra": "2759596 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression - allocs/op",
            "value": 34,
            "unit": "allocs/op",
            "extra": "2759596 times"
          },
          {
            "name": "BenchmarkPatternMatcher",
            "value": 3831,
            "unit": "ns/op\t    5952 B/op\t      50 allocs/op",
            "extra": "1569428 times"
          },
          {
            "name": "BenchmarkPatternMatcher - ns/op",
            "value": 3831,
            "unit": "ns/op",
            "extra": "1569428 times"
          },
          {
            "name": "BenchmarkPatternMatcher - B/op",
            "value": 5952,
            "unit": "B/op",
            "extra": "1569428 times"
          },
          {
            "name": "BenchmarkPatternMatcher - allocs/op",
            "value": 50,
            "unit": "allocs/op",
            "extra": "1569428 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1",
            "value": 4402616,
            "unit": "ns/op\t  297700 B/op\t    4482 allocs/op",
            "extra": "1896 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1 - ns/op",
            "value": 4402616,
            "unit": "ns/op",
            "extra": "1896 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1 - B/op",
            "value": 297700,
            "unit": "B/op",
            "extra": "1896 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 4482,
            "unit": "allocs/op",
            "extra": "1896 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1",
            "value": 2323414,
            "unit": "ns/op\t  307908 B/op\t    4660 allocs/op",
            "extra": "3145 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1 - ns/op",
            "value": 2323414,
            "unit": "ns/op",
            "extra": "3145 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1 - B/op",
            "value": 307908,
            "unit": "B/op",
            "extra": "3145 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1 - allocs/op",
            "value": 4660,
            "unit": "allocs/op",
            "extra": "3145 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1",
            "value": 961579,
            "unit": "ns/op\t  236628 B/op\t    3680 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1 - ns/op",
            "value": 961579,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1 - B/op",
            "value": 236628,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1 - allocs/op",
            "value": 3680,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1",
            "value": 10664401,
            "unit": "ns/op\t  549982 B/op\t    7723 allocs/op",
            "extra": "686 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1 - ns/op",
            "value": 10664401,
            "unit": "ns/op",
            "extra": "686 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1 - B/op",
            "value": 549982,
            "unit": "B/op",
            "extra": "686 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1 - allocs/op",
            "value": 7723,
            "unit": "allocs/op",
            "extra": "686 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1",
            "value": 5222672,
            "unit": "ns/op\t  589109 B/op\t    8330 allocs/op",
            "extra": "1370 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1 - ns/op",
            "value": 5222672,
            "unit": "ns/op",
            "extra": "1370 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1 - B/op",
            "value": 589109,
            "unit": "B/op",
            "extra": "1370 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1 - allocs/op",
            "value": 8330,
            "unit": "allocs/op",
            "extra": "1370 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1",
            "value": 1413912,
            "unit": "ns/op\t  331236 B/op\t    4695 allocs/op",
            "extra": "6852 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1 - ns/op",
            "value": 1413912,
            "unit": "ns/op",
            "extra": "6852 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1 - B/op",
            "value": 331236,
            "unit": "B/op",
            "extra": "6852 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1 - allocs/op",
            "value": 4695,
            "unit": "allocs/op",
            "extra": "6852 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1",
            "value": 20036614,
            "unit": "ns/op\t 1891041 B/op\t   24221 allocs/op",
            "extra": "375 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1 - ns/op",
            "value": 20036614,
            "unit": "ns/op",
            "extra": "375 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1 - B/op",
            "value": 1891041,
            "unit": "B/op",
            "extra": "375 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1 - allocs/op",
            "value": 24221,
            "unit": "allocs/op",
            "extra": "375 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1",
            "value": 13169754,
            "unit": "ns/op\t 1992668 B/op\t   25602 allocs/op",
            "extra": "628 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1 - ns/op",
            "value": 13169754,
            "unit": "ns/op",
            "extra": "628 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1 - B/op",
            "value": 1992668,
            "unit": "B/op",
            "extra": "628 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1 - allocs/op",
            "value": 25602,
            "unit": "allocs/op",
            "extra": "628 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1",
            "value": 6535673,
            "unit": "ns/op\t 1280887 B/op\t   14068 allocs/op",
            "extra": "1705 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1 - ns/op",
            "value": 6535673,
            "unit": "ns/op",
            "extra": "1705 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1 - B/op",
            "value": 1280887,
            "unit": "B/op",
            "extra": "1705 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1 - allocs/op",
            "value": 14068,
            "unit": "allocs/op",
            "extra": "1705 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1",
            "value": 42887121,
            "unit": "ns/op\t 9377155 B/op\t  127424 allocs/op",
            "extra": "134 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1 - ns/op",
            "value": 42887121,
            "unit": "ns/op",
            "extra": "134 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1 - B/op",
            "value": 9377155,
            "unit": "B/op",
            "extra": "134 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1 - allocs/op",
            "value": 127424,
            "unit": "allocs/op",
            "extra": "134 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1",
            "value": 37258420,
            "unit": "ns/op\t 9399641 B/op\t  129836 allocs/op",
            "extra": "165 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1 - ns/op",
            "value": 37258420,
            "unit": "ns/op",
            "extra": "165 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1 - B/op",
            "value": 9399641,
            "unit": "B/op",
            "extra": "165 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1 - allocs/op",
            "value": 129836,
            "unit": "allocs/op",
            "extra": "165 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1",
            "value": 33888023,
            "unit": "ns/op\t 9290415 B/op\t  126235 allocs/op",
            "extra": "178 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1 - ns/op",
            "value": 33888023,
            "unit": "ns/op",
            "extra": "178 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1 - B/op",
            "value": 9290415,
            "unit": "B/op",
            "extra": "178 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1 - allocs/op",
            "value": 126235,
            "unit": "allocs/op",
            "extra": "178 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1",
            "value": 5795850,
            "unit": "ns/op\t  267464 B/op\t    4077 allocs/op",
            "extra": "1468 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1 - ns/op",
            "value": 5795850,
            "unit": "ns/op",
            "extra": "1468 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1 - B/op",
            "value": 267464,
            "unit": "B/op",
            "extra": "1468 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 4077,
            "unit": "allocs/op",
            "extra": "1468 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1",
            "value": 2133068,
            "unit": "ns/op\t  280637 B/op\t    4316 allocs/op",
            "extra": "3051 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1 - ns/op",
            "value": 2133068,
            "unit": "ns/op",
            "extra": "3051 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1 - B/op",
            "value": 280637,
            "unit": "B/op",
            "extra": "3051 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1 - allocs/op",
            "value": 4316,
            "unit": "allocs/op",
            "extra": "3051 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1",
            "value": 593547,
            "unit": "ns/op\t  201909 B/op\t    3367 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1 - ns/op",
            "value": 593547,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1 - B/op",
            "value": 201909,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1 - allocs/op",
            "value": 3367,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1",
            "value": 12399523,
            "unit": "ns/op\t  861414 B/op\t   13490 allocs/op",
            "extra": "610 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1 - ns/op",
            "value": 12399523,
            "unit": "ns/op",
            "extra": "610 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1 - B/op",
            "value": 861414,
            "unit": "B/op",
            "extra": "610 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 13490,
            "unit": "allocs/op",
            "extra": "610 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1",
            "value": 4755831,
            "unit": "ns/op\t  915814 B/op\t   14462 allocs/op",
            "extra": "1206 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1 - ns/op",
            "value": 4755831,
            "unit": "ns/op",
            "extra": "1206 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1 - B/op",
            "value": 915814,
            "unit": "B/op",
            "extra": "1206 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1 - allocs/op",
            "value": 14462,
            "unit": "allocs/op",
            "extra": "1206 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1",
            "value": 1411261,
            "unit": "ns/op\t  657771 B/op\t   11777 allocs/op",
            "extra": "4068 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1 - ns/op",
            "value": 1411261,
            "unit": "ns/op",
            "extra": "4068 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1 - B/op",
            "value": 657771,
            "unit": "B/op",
            "extra": "4068 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1 - allocs/op",
            "value": 11777,
            "unit": "allocs/op",
            "extra": "4068 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1",
            "value": 12957845,
            "unit": "ns/op\t  582482 B/op\t    7905 allocs/op",
            "extra": "434 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1 - ns/op",
            "value": 12957845,
            "unit": "ns/op",
            "extra": "434 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1 - B/op",
            "value": 582482,
            "unit": "B/op",
            "extra": "434 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 7905,
            "unit": "allocs/op",
            "extra": "434 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1",
            "value": 4171587,
            "unit": "ns/op\t  633312 B/op\t    8516 allocs/op",
            "extra": "1392 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1 - ns/op",
            "value": 4171587,
            "unit": "ns/op",
            "extra": "1392 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1 - B/op",
            "value": 633312,
            "unit": "B/op",
            "extra": "1392 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1 - allocs/op",
            "value": 8516,
            "unit": "allocs/op",
            "extra": "1392 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1",
            "value": 787736,
            "unit": "ns/op\t  344251 B/op\t    4317 allocs/op",
            "extra": "7208 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1 - ns/op",
            "value": 787736,
            "unit": "ns/op",
            "extra": "7208 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1 - B/op",
            "value": 344251,
            "unit": "B/op",
            "extra": "7208 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1 - allocs/op",
            "value": 4317,
            "unit": "allocs/op",
            "extra": "7208 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1",
            "value": 4058799,
            "unit": "ns/op\t  131090 B/op\t    2018 allocs/op",
            "extra": "2362 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1 - ns/op",
            "value": 4058799,
            "unit": "ns/op",
            "extra": "2362 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1 - B/op",
            "value": 131090,
            "unit": "B/op",
            "extra": "2362 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1 - allocs/op",
            "value": 2018,
            "unit": "allocs/op",
            "extra": "2362 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1",
            "value": 2684002,
            "unit": "ns/op\t  137120 B/op\t    2105 allocs/op",
            "extra": "2208 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1 - ns/op",
            "value": 2684002,
            "unit": "ns/op",
            "extra": "2208 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1 - B/op",
            "value": 137120,
            "unit": "B/op",
            "extra": "2208 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1 - allocs/op",
            "value": 2105,
            "unit": "allocs/op",
            "extra": "2208 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1",
            "value": 232413,
            "unit": "ns/op\t   85814 B/op\t    1386 allocs/op",
            "extra": "25970 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1 - ns/op",
            "value": 232413,
            "unit": "ns/op",
            "extra": "25970 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1 - B/op",
            "value": 85814,
            "unit": "B/op",
            "extra": "25970 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1 - allocs/op",
            "value": 1386,
            "unit": "allocs/op",
            "extra": "25970 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph",
            "value": 8777,
            "unit": "ns/op\t    6824 B/op\t      98 allocs/op",
            "extra": "694792 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph - ns/op",
            "value": 8777,
            "unit": "ns/op",
            "extra": "694792 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph - B/op",
            "value": 6824,
            "unit": "B/op",
            "extra": "694792 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph - allocs/op",
            "value": 98,
            "unit": "allocs/op",
            "extra": "694792 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph",
            "value": 31078,
            "unit": "ns/op\t   26536 B/op\t     241 allocs/op",
            "extra": "194474 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph - ns/op",
            "value": 31078,
            "unit": "ns/op",
            "extra": "194474 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph - B/op",
            "value": 26536,
            "unit": "B/op",
            "extra": "194474 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph - allocs/op",
            "value": 241,
            "unit": "allocs/op",
            "extra": "194474 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph",
            "value": 29704,
            "unit": "ns/op\t   22952 B/op\t     281 allocs/op",
            "extra": "207663 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph - ns/op",
            "value": 29704,
            "unit": "ns/op",
            "extra": "207663 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph - B/op",
            "value": 22952,
            "unit": "B/op",
            "extra": "207663 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph - allocs/op",
            "value": 281,
            "unit": "allocs/op",
            "extra": "207663 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph",
            "value": 2226,
            "unit": "ns/op\t    1368 B/op\t      32 allocs/op",
            "extra": "2702152 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph - ns/op",
            "value": 2226,
            "unit": "ns/op",
            "extra": "2702152 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph - B/op",
            "value": 1368,
            "unit": "B/op",
            "extra": "2702152 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph - allocs/op",
            "value": 32,
            "unit": "allocs/op",
            "extra": "2702152 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph",
            "value": 38063,
            "unit": "ns/op\t   39505 B/op\t     228 allocs/op",
            "extra": "153075 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph - ns/op",
            "value": 38063,
            "unit": "ns/op",
            "extra": "153075 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph - B/op",
            "value": 39505,
            "unit": "B/op",
            "extra": "153075 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph - allocs/op",
            "value": 228,
            "unit": "allocs/op",
            "extra": "153075 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph",
            "value": 8909,
            "unit": "ns/op\t    6800 B/op\t      99 allocs/op",
            "extra": "676267 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph - ns/op",
            "value": 8909,
            "unit": "ns/op",
            "extra": "676267 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph - B/op",
            "value": 6800,
            "unit": "B/op",
            "extra": "676267 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph - allocs/op",
            "value": 99,
            "unit": "allocs/op",
            "extra": "676267 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources",
            "value": 10488,
            "unit": "ns/op\t    7656 B/op\t     123 allocs/op",
            "extra": "561993 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources - ns/op",
            "value": 10488,
            "unit": "ns/op",
            "extra": "561993 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources - B/op",
            "value": 7656,
            "unit": "B/op",
            "extra": "561993 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources - allocs/op",
            "value": 123,
            "unit": "allocs/op",
            "extra": "561993 times"
          },
          {
            "name": "BenchmarkRelationsReferencing",
            "value": 18.35,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "00000325648992 times"
          },
          {
            "name": "BenchmarkRelationsReferencing - ns/op",
            "value": 18.35,
            "unit": "ns/op",
            "extra": "00000325648992 times"
          },
          {
            "name": "BenchmarkRelationsReferencing - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "00000325648992 times"
          },
          {
            "name": "BenchmarkRelationsReferencing - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "00000325648992 times"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "barak.michener@authzed.com",
            "name": "Barak Michener",
            "username": "barakmich"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ea09c1e1fa3d3cd51c356e0cc3ce370d3910f885",
          "message": "fix: update IterSubjects for wildcards and Alias iterators for confomance (#2864)",
          "timestamp": "2026-02-03T13:09:40-08:00",
          "tree_id": "b8ca3d1385d829f5401accc9a58fd07199121ffc",
          "url": "https://github.com/authzed/spicedb/commit/ea09c1e1fa3d3cd51c356e0cc3ce370d3910f885"
        },
        "date": 1770153747344,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead",
            "value": 386186,
            "unit": "ns/op\t   15340 B/op\t     221 allocs/op",
            "extra": "15462 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead - ns/op",
            "value": 386186,
            "unit": "ns/op",
            "extra": "15462 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead - B/op",
            "value": 15340,
            "unit": "B/op",
            "extra": "15462 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead - allocs/op",
            "value": 221,
            "unit": "allocs/op",
            "extra": "15462 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace",
            "value": 8132130,
            "unit": "ns/op\t   94219 B/op\t   20135 allocs/op",
            "extra": "732 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace - ns/op",
            "value": 8132130,
            "unit": "ns/op",
            "extra": "732 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace - B/op",
            "value": 94219,
            "unit": "B/op",
            "extra": "732 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace - allocs/op",
            "value": 20135,
            "unit": "allocs/op",
            "extra": "732 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource",
            "value": 8202457,
            "unit": "ns/op\t   97328 B/op\t   20194 allocs/op",
            "extra": "715 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - ns/op",
            "value": 8202457,
            "unit": "ns/op",
            "extra": "715 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - B/op",
            "value": 97328,
            "unit": "B/op",
            "extra": "715 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "715 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject",
            "value": 10220304,
            "unit": "ns/op\t   97323 B/op\t   20194 allocs/op",
            "extra": "574 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - ns/op",
            "value": 10220304,
            "unit": "ns/op",
            "extra": "574 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - B/op",
            "value": 97323,
            "unit": "B/op",
            "extra": "574 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "574 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource",
            "value": 8009240,
            "unit": "ns/op\t   77880 B/op\t   15210 allocs/op",
            "extra": "748 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource - ns/op",
            "value": 8009240,
            "unit": "ns/op",
            "extra": "748 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource - B/op",
            "value": 77880,
            "unit": "B/op",
            "extra": "748 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource - allocs/op",
            "value": 15210,
            "unit": "allocs/op",
            "extra": "748 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject",
            "value": 9855431,
            "unit": "ns/op\t   77846 B/op\t   15210 allocs/op",
            "extra": "600 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject - ns/op",
            "value": 9855431,
            "unit": "ns/op",
            "extra": "600 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject - B/op",
            "value": 77846,
            "unit": "B/op",
            "extra": "600 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject - allocs/op",
            "value": 15210,
            "unit": "allocs/op",
            "extra": "600 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject",
            "value": 555688,
            "unit": "ns/op\t   18429 B/op\t     280 allocs/op",
            "extra": "9968 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - ns/op",
            "value": 555688,
            "unit": "ns/op",
            "extra": "9968 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - B/op",
            "value": 18429,
            "unit": "B/op",
            "extra": "9968 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "9968 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource",
            "value": 514847,
            "unit": "ns/op\t   18431 B/op\t     280 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - ns/op",
            "value": 514847,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - B/op",
            "value": 18431,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead",
            "value": 7796269,
            "unit": "ns/op\t  172700 B/op\t   20202 allocs/op",
            "extra": "757 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead - ns/op",
            "value": 7796269,
            "unit": "ns/op",
            "extra": "757 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead - B/op",
            "value": 172700,
            "unit": "B/op",
            "extra": "757 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead - allocs/op",
            "value": 20202,
            "unit": "allocs/op",
            "extra": "757 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch",
            "value": 17342119,
            "unit": "ns/op\t   23491 B/op\t     298 allocs/op",
            "extra": "356 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch - ns/op",
            "value": 17342119,
            "unit": "ns/op",
            "extra": "356 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch - B/op",
            "value": 23491,
            "unit": "B/op",
            "extra": "356 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch - allocs/op",
            "value": 298,
            "unit": "allocs/op",
            "extra": "356 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create",
            "value": 18653274,
            "unit": "ns/op\t   20842 B/op\t     292 allocs/op",
            "extra": "350 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create - ns/op",
            "value": 18653274,
            "unit": "ns/op",
            "extra": "350 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create - B/op",
            "value": 20842,
            "unit": "B/op",
            "extra": "350 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create - allocs/op",
            "value": 292,
            "unit": "allocs/op",
            "extra": "350 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_",
            "value": 23936556,
            "unit": "ns/op\t 4465623 B/op\t   45847 allocs/op",
            "extra": "224 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_ - ns/op",
            "value": 23936556,
            "unit": "ns/op",
            "extra": "224 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_ - B/op",
            "value": 4465623,
            "unit": "B/op",
            "extra": "224 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_ - allocs/op",
            "value": 45847,
            "unit": "allocs/op",
            "extra": "224 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_",
            "value": 37152074,
            "unit": "ns/op\t 4511202 B/op\t   45984 allocs/op",
            "extra": "168 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_ - ns/op",
            "value": 37152074,
            "unit": "ns/op",
            "extra": "168 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_ - B/op",
            "value": 4511202,
            "unit": "B/op",
            "extra": "168 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_ - allocs/op",
            "value": 45984,
            "unit": "allocs/op",
            "extra": "168 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_",
            "value": 42506095,
            "unit": "ns/op\t 4442717 B/op\t   46444 allocs/op",
            "extra": "128 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_ - ns/op",
            "value": 42506095,
            "unit": "ns/op",
            "extra": "128 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_ - B/op",
            "value": 4442717,
            "unit": "B/op",
            "extra": "128 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_ - allocs/op",
            "value": 46444,
            "unit": "allocs/op",
            "extra": "128 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_",
            "value": 46725205,
            "unit": "ns/op\t 4656328 B/op\t   46960 allocs/op",
            "extra": "124 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_ - ns/op",
            "value": 46725205,
            "unit": "ns/op",
            "extra": "124 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_ - B/op",
            "value": 4656328,
            "unit": "B/op",
            "extra": "124 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_ - allocs/op",
            "value": 46960,
            "unit": "allocs/op",
            "extra": "124 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_",
            "value": 67390956,
            "unit": "ns/op\t 5173737 B/op\t   48107 allocs/op",
            "extra": "76 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_ - ns/op",
            "value": 67390956,
            "unit": "ns/op",
            "extra": "76 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_ - B/op",
            "value": 5173737,
            "unit": "B/op",
            "extra": "76 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_ - allocs/op",
            "value": 48107,
            "unit": "allocs/op",
            "extra": "76 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead",
            "value": 389617,
            "unit": "ns/op\t   15340 B/op\t     221 allocs/op",
            "extra": "15405 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead - ns/op",
            "value": 389617,
            "unit": "ns/op",
            "extra": "15405 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead - B/op",
            "value": 15340,
            "unit": "B/op",
            "extra": "15405 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead - allocs/op",
            "value": 221,
            "unit": "allocs/op",
            "extra": "15405 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace",
            "value": 8170501,
            "unit": "ns/op\t   94229 B/op\t   20135 allocs/op",
            "extra": "736 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace - ns/op",
            "value": 8170501,
            "unit": "ns/op",
            "extra": "736 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace - B/op",
            "value": 94229,
            "unit": "B/op",
            "extra": "736 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace - allocs/op",
            "value": 20135,
            "unit": "allocs/op",
            "extra": "736 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource",
            "value": 8165204,
            "unit": "ns/op\t   97314 B/op\t   20194 allocs/op",
            "extra": "734 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - ns/op",
            "value": 8165204,
            "unit": "ns/op",
            "extra": "734 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - B/op",
            "value": 97314,
            "unit": "B/op",
            "extra": "734 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "734 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject",
            "value": 10242946,
            "unit": "ns/op\t   97296 B/op\t   20194 allocs/op",
            "extra": "579 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - ns/op",
            "value": 10242946,
            "unit": "ns/op",
            "extra": "579 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - B/op",
            "value": 97296,
            "unit": "B/op",
            "extra": "579 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "579 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject",
            "value": 9969401,
            "unit": "ns/op\t   77809 B/op\t   15209 allocs/op",
            "extra": "597 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject - ns/op",
            "value": 9969401,
            "unit": "ns/op",
            "extra": "597 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject - B/op",
            "value": 77809,
            "unit": "B/op",
            "extra": "597 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject - allocs/op",
            "value": 15209,
            "unit": "allocs/op",
            "extra": "597 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource",
            "value": 8042742,
            "unit": "ns/op\t   77798 B/op\t   15209 allocs/op",
            "extra": "741 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource - ns/op",
            "value": 8042742,
            "unit": "ns/op",
            "extra": "741 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource - B/op",
            "value": 77798,
            "unit": "B/op",
            "extra": "741 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource - allocs/op",
            "value": 15209,
            "unit": "allocs/op",
            "extra": "741 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource",
            "value": 480996,
            "unit": "ns/op\t   18431 B/op\t     280 allocs/op",
            "extra": "14589 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - ns/op",
            "value": 480996,
            "unit": "ns/op",
            "extra": "14589 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - B/op",
            "value": 18431,
            "unit": "B/op",
            "extra": "14589 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "14589 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject",
            "value": 536910,
            "unit": "ns/op\t   18430 B/op\t     280 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - ns/op",
            "value": 536910,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - B/op",
            "value": 18430,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead",
            "value": 7846512,
            "unit": "ns/op\t  172715 B/op\t   20202 allocs/op",
            "extra": "764 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead - ns/op",
            "value": 7846512,
            "unit": "ns/op",
            "extra": "764 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead - B/op",
            "value": 172715,
            "unit": "B/op",
            "extra": "764 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead - allocs/op",
            "value": 20202,
            "unit": "allocs/op",
            "extra": "764 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch",
            "value": 3411639,
            "unit": "ns/op\t   20939 B/op\t     272 allocs/op",
            "extra": "1512 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch - ns/op",
            "value": 3411639,
            "unit": "ns/op",
            "extra": "1512 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch - B/op",
            "value": 20939,
            "unit": "B/op",
            "extra": "1512 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch - allocs/op",
            "value": 272,
            "unit": "allocs/op",
            "extra": "1512 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create",
            "value": 2236747,
            "unit": "ns/op\t   18286 B/op\t     266 allocs/op",
            "extra": "2822 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create - ns/op",
            "value": 2236747,
            "unit": "ns/op",
            "extra": "2822 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create - B/op",
            "value": 18286,
            "unit": "B/op",
            "extra": "2822 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create - allocs/op",
            "value": 266,
            "unit": "allocs/op",
            "extra": "2822 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_",
            "value": 16687128,
            "unit": "ns/op\t 4462107 B/op\t   45823 allocs/op",
            "extra": "346 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_ - ns/op",
            "value": 16687128,
            "unit": "ns/op",
            "extra": "346 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_ - B/op",
            "value": 4462107,
            "unit": "B/op",
            "extra": "346 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_ - allocs/op",
            "value": 45823,
            "unit": "allocs/op",
            "extra": "346 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_",
            "value": 26433830,
            "unit": "ns/op\t 4509849 B/op\t   45959 allocs/op",
            "extra": "228 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_ - ns/op",
            "value": 26433830,
            "unit": "ns/op",
            "extra": "228 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_ - B/op",
            "value": 4509849,
            "unit": "B/op",
            "extra": "228 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_ - allocs/op",
            "value": 45959,
            "unit": "allocs/op",
            "extra": "228 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_",
            "value": 31789398,
            "unit": "ns/op\t 4440199 B/op\t   46420 allocs/op",
            "extra": "177 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_ - ns/op",
            "value": 31789398,
            "unit": "ns/op",
            "extra": "177 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_ - B/op",
            "value": 4440199,
            "unit": "B/op",
            "extra": "177 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_ - allocs/op",
            "value": 46420,
            "unit": "allocs/op",
            "extra": "177 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_",
            "value": 45089105,
            "unit": "ns/op\t 4653417 B/op\t   46931 allocs/op",
            "extra": "129 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_ - ns/op",
            "value": 45089105,
            "unit": "ns/op",
            "extra": "129 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_ - B/op",
            "value": 4653417,
            "unit": "B/op",
            "extra": "129 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_ - allocs/op",
            "value": 46931,
            "unit": "allocs/op",
            "extra": "129 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_",
            "value": 66044284,
            "unit": "ns/op\t 5170983 B/op\t   48086 allocs/op",
            "extra": "86 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_ - ns/op",
            "value": 66044284,
            "unit": "ns/op",
            "extra": "86 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_ - B/op",
            "value": 5170983,
            "unit": "B/op",
            "extra": "86 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_ - allocs/op",
            "value": 48086,
            "unit": "allocs/op",
            "extra": "86 times"
          },
          {
            "name": "BenchmarkQueryRelationships",
            "value": 438749,
            "unit": "ns/op\t     696 B/op\t      18 allocs/op",
            "extra": "13604 times"
          },
          {
            "name": "BenchmarkQueryRelationships - ns/op",
            "value": 438749,
            "unit": "ns/op",
            "extra": "13604 times"
          },
          {
            "name": "BenchmarkQueryRelationships - B/op",
            "value": 696,
            "unit": "B/op",
            "extra": "13604 times"
          },
          {
            "name": "BenchmarkQueryRelationships - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "13604 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true",
            "value": 2759760,
            "unit": "ns/op\t 2245615 B/op\t   17080 allocs/op",
            "extra": "2258 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true - ns/op",
            "value": 2759760,
            "unit": "ns/op",
            "extra": "2258 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true - B/op",
            "value": 2245615,
            "unit": "B/op",
            "extra": "2258 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true - allocs/op",
            "value": 17080,
            "unit": "allocs/op",
            "extra": "2258 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false",
            "value": 2216522,
            "unit": "ns/op\t 2101433 B/op\t   14076 allocs/op",
            "extra": "2673 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false - ns/op",
            "value": 2216522,
            "unit": "ns/op",
            "extra": "2673 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false - B/op",
            "value": 2101433,
            "unit": "B/op",
            "extra": "2673 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false - allocs/op",
            "value": 14076,
            "unit": "allocs/op",
            "extra": "2673 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash",
            "value": 607.2,
            "unit": "ns/op\t     144 B/op\t       3 allocs/op",
            "extra": "9867836 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash - ns/op",
            "value": 607.2,
            "unit": "ns/op",
            "extra": "9867836 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash - B/op",
            "value": 144,
            "unit": "B/op",
            "extra": "9867836 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9867836 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions",
            "value": 141,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "43277108 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions - ns/op",
            "value": 141,
            "unit": "ns/op",
            "extra": "43277108 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "43277108 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "43277108 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching",
            "value": 26443,
            "unit": "ns/op\t    3977 B/op\t      72 allocs/op",
            "extra": "315355 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching - ns/op",
            "value": 26443,
            "unit": "ns/op",
            "extra": "315355 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching - B/op",
            "value": 3977,
            "unit": "B/op",
            "extra": "315355 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching - allocs/op",
            "value": 72,
            "unit": "allocs/op",
            "extra": "315355 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression",
            "value": 1982,
            "unit": "ns/op\t    1577 B/op\t      34 allocs/op",
            "extra": "3027434 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression - ns/op",
            "value": 1982,
            "unit": "ns/op",
            "extra": "3027434 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression - B/op",
            "value": 1577,
            "unit": "B/op",
            "extra": "3027434 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression - allocs/op",
            "value": 34,
            "unit": "allocs/op",
            "extra": "3027434 times"
          },
          {
            "name": "BenchmarkPatternMatcher",
            "value": 3575,
            "unit": "ns/op\t    5952 B/op\t      50 allocs/op",
            "extra": "1700188 times"
          },
          {
            "name": "BenchmarkPatternMatcher - ns/op",
            "value": 3575,
            "unit": "ns/op",
            "extra": "1700188 times"
          },
          {
            "name": "BenchmarkPatternMatcher - B/op",
            "value": 5952,
            "unit": "B/op",
            "extra": "1700188 times"
          },
          {
            "name": "BenchmarkPatternMatcher - allocs/op",
            "value": 50,
            "unit": "allocs/op",
            "extra": "1700188 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1",
            "value": 3562728,
            "unit": "ns/op\t  297769 B/op\t    4483 allocs/op",
            "extra": "2293 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1 - ns/op",
            "value": 3562728,
            "unit": "ns/op",
            "extra": "2293 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1 - B/op",
            "value": 297769,
            "unit": "B/op",
            "extra": "2293 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 4483,
            "unit": "allocs/op",
            "extra": "2293 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1",
            "value": 1897990,
            "unit": "ns/op\t  307928 B/op\t    4660 allocs/op",
            "extra": "3811 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1 - ns/op",
            "value": 1897990,
            "unit": "ns/op",
            "extra": "3811 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1 - B/op",
            "value": 307928,
            "unit": "B/op",
            "extra": "3811 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1 - allocs/op",
            "value": 4660,
            "unit": "allocs/op",
            "extra": "3811 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1",
            "value": 889259,
            "unit": "ns/op\t  236559 B/op\t    3680 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1 - ns/op",
            "value": 889259,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1 - B/op",
            "value": 236559,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1 - allocs/op",
            "value": 3680,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1",
            "value": 9008406,
            "unit": "ns/op\t  548587 B/op\t    7691 allocs/op",
            "extra": "787 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1 - ns/op",
            "value": 9008406,
            "unit": "ns/op",
            "extra": "787 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1 - B/op",
            "value": 548587,
            "unit": "B/op",
            "extra": "787 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1 - allocs/op",
            "value": 7691,
            "unit": "allocs/op",
            "extra": "787 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1",
            "value": 4483998,
            "unit": "ns/op\t  588726 B/op\t    8319 allocs/op",
            "extra": "1628 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1 - ns/op",
            "value": 4483998,
            "unit": "ns/op",
            "extra": "1628 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1 - B/op",
            "value": 588726,
            "unit": "B/op",
            "extra": "1628 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1 - allocs/op",
            "value": 8319,
            "unit": "allocs/op",
            "extra": "1628 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1",
            "value": 1190591,
            "unit": "ns/op\t  331316 B/op\t    4691 allocs/op",
            "extra": "8826 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1 - ns/op",
            "value": 1190591,
            "unit": "ns/op",
            "extra": "8826 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1 - B/op",
            "value": 331316,
            "unit": "B/op",
            "extra": "8826 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1 - allocs/op",
            "value": 4691,
            "unit": "allocs/op",
            "extra": "8826 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1",
            "value": 18096275,
            "unit": "ns/op\t 1891987 B/op\t   24250 allocs/op",
            "extra": "436 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1 - ns/op",
            "value": 18096275,
            "unit": "ns/op",
            "extra": "436 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1 - B/op",
            "value": 1891987,
            "unit": "B/op",
            "extra": "436 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1 - allocs/op",
            "value": 24250,
            "unit": "allocs/op",
            "extra": "436 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1",
            "value": 11283123,
            "unit": "ns/op\t 1986157 B/op\t   25496 allocs/op",
            "extra": "732 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1 - ns/op",
            "value": 11283123,
            "unit": "ns/op",
            "extra": "732 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1 - B/op",
            "value": 1986157,
            "unit": "B/op",
            "extra": "732 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1 - allocs/op",
            "value": 25496,
            "unit": "allocs/op",
            "extra": "732 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1",
            "value": 5419374,
            "unit": "ns/op\t 1280351 B/op\t   14055 allocs/op",
            "extra": "1900 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1 - ns/op",
            "value": 5419374,
            "unit": "ns/op",
            "extra": "1900 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1 - B/op",
            "value": 1280351,
            "unit": "B/op",
            "extra": "1900 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1 - allocs/op",
            "value": 14055,
            "unit": "allocs/op",
            "extra": "1900 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1",
            "value": 38773895,
            "unit": "ns/op\t 9375898 B/op\t  127382 allocs/op",
            "extra": "252 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1 - ns/op",
            "value": 38773895,
            "unit": "ns/op",
            "extra": "252 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1 - B/op",
            "value": 9375898,
            "unit": "B/op",
            "extra": "252 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1 - allocs/op",
            "value": 127382,
            "unit": "allocs/op",
            "extra": "252 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1",
            "value": 34267108,
            "unit": "ns/op\t 9401857 B/op\t  129912 allocs/op",
            "extra": "175 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1 - ns/op",
            "value": 34267108,
            "unit": "ns/op",
            "extra": "175 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1 - B/op",
            "value": 9401857,
            "unit": "B/op",
            "extra": "175 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1 - allocs/op",
            "value": 129912,
            "unit": "allocs/op",
            "extra": "175 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1",
            "value": 30408098,
            "unit": "ns/op\t 9292768 B/op\t  126308 allocs/op",
            "extra": "193 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1 - ns/op",
            "value": 30408098,
            "unit": "ns/op",
            "extra": "193 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1 - B/op",
            "value": 9292768,
            "unit": "B/op",
            "extra": "193 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1 - allocs/op",
            "value": 126308,
            "unit": "allocs/op",
            "extra": "193 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1",
            "value": 4404866,
            "unit": "ns/op\t  265168 B/op\t    4044 allocs/op",
            "extra": "1866 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1 - ns/op",
            "value": 4404866,
            "unit": "ns/op",
            "extra": "1866 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1 - B/op",
            "value": 265168,
            "unit": "B/op",
            "extra": "1866 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 4044,
            "unit": "allocs/op",
            "extra": "1866 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1",
            "value": 1850320,
            "unit": "ns/op\t  280654 B/op\t    4316 allocs/op",
            "extra": "3811 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1 - ns/op",
            "value": 1850320,
            "unit": "ns/op",
            "extra": "3811 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1 - B/op",
            "value": 280654,
            "unit": "B/op",
            "extra": "3811 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1 - allocs/op",
            "value": 4316,
            "unit": "allocs/op",
            "extra": "3811 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1",
            "value": 554801,
            "unit": "ns/op\t  201912 B/op\t    3367 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1 - ns/op",
            "value": 554801,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1 - B/op",
            "value": 201912,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1 - allocs/op",
            "value": 3367,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1",
            "value": 10780095,
            "unit": "ns/op\t  861393 B/op\t   13487 allocs/op",
            "extra": "668 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1 - ns/op",
            "value": 10780095,
            "unit": "ns/op",
            "extra": "668 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1 - B/op",
            "value": 861393,
            "unit": "B/op",
            "extra": "668 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 13487,
            "unit": "allocs/op",
            "extra": "668 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1",
            "value": 4282564,
            "unit": "ns/op\t  915824 B/op\t   14462 allocs/op",
            "extra": "1362 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1 - ns/op",
            "value": 4282564,
            "unit": "ns/op",
            "extra": "1362 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1 - B/op",
            "value": 915824,
            "unit": "B/op",
            "extra": "1362 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1 - allocs/op",
            "value": 14462,
            "unit": "allocs/op",
            "extra": "1362 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1",
            "value": 1302375,
            "unit": "ns/op\t  657773 B/op\t   11777 allocs/op",
            "extra": "4377 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1 - ns/op",
            "value": 1302375,
            "unit": "ns/op",
            "extra": "4377 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1 - B/op",
            "value": 657773,
            "unit": "B/op",
            "extra": "4377 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1 - allocs/op",
            "value": 11777,
            "unit": "allocs/op",
            "extra": "4377 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1",
            "value": 11018727,
            "unit": "ns/op\t  582384 B/op\t    7908 allocs/op",
            "extra": "524 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1 - ns/op",
            "value": 11018727,
            "unit": "ns/op",
            "extra": "524 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1 - B/op",
            "value": 582384,
            "unit": "B/op",
            "extra": "524 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 7908,
            "unit": "allocs/op",
            "extra": "524 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1",
            "value": 3843360,
            "unit": "ns/op\t  633317 B/op\t    8516 allocs/op",
            "extra": "1502 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1 - ns/op",
            "value": 3843360,
            "unit": "ns/op",
            "extra": "1502 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1 - B/op",
            "value": 633317,
            "unit": "B/op",
            "extra": "1502 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1 - allocs/op",
            "value": 8516,
            "unit": "allocs/op",
            "extra": "1502 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1",
            "value": 764824,
            "unit": "ns/op\t  344251 B/op\t    4317 allocs/op",
            "extra": "7002 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1 - ns/op",
            "value": 764824,
            "unit": "ns/op",
            "extra": "7002 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1 - B/op",
            "value": 344251,
            "unit": "B/op",
            "extra": "7002 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1 - allocs/op",
            "value": 4317,
            "unit": "allocs/op",
            "extra": "7002 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1",
            "value": 3277515,
            "unit": "ns/op\t  130685 B/op\t    2009 allocs/op",
            "extra": "2774 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1 - ns/op",
            "value": 3277515,
            "unit": "ns/op",
            "extra": "2774 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1 - B/op",
            "value": 130685,
            "unit": "B/op",
            "extra": "2774 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1 - allocs/op",
            "value": 2009,
            "unit": "allocs/op",
            "extra": "2774 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1",
            "value": 2452470,
            "unit": "ns/op\t  137119 B/op\t    2105 allocs/op",
            "extra": "2431 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1 - ns/op",
            "value": 2452470,
            "unit": "ns/op",
            "extra": "2431 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1 - B/op",
            "value": 137119,
            "unit": "B/op",
            "extra": "2431 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1 - allocs/op",
            "value": 2105,
            "unit": "allocs/op",
            "extra": "2431 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1",
            "value": 219631,
            "unit": "ns/op\t   85814 B/op\t    1386 allocs/op",
            "extra": "27028 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1 - ns/op",
            "value": 219631,
            "unit": "ns/op",
            "extra": "27028 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1 - B/op",
            "value": 85814,
            "unit": "B/op",
            "extra": "27028 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1 - allocs/op",
            "value": 1386,
            "unit": "allocs/op",
            "extra": "27028 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph",
            "value": 8328,
            "unit": "ns/op\t    6824 B/op\t      98 allocs/op",
            "extra": "723585 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph - ns/op",
            "value": 8328,
            "unit": "ns/op",
            "extra": "723585 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph - B/op",
            "value": 6824,
            "unit": "B/op",
            "extra": "723585 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph - allocs/op",
            "value": 98,
            "unit": "allocs/op",
            "extra": "723585 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph",
            "value": 29314,
            "unit": "ns/op\t   26536 B/op\t     241 allocs/op",
            "extra": "204820 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph - ns/op",
            "value": 29314,
            "unit": "ns/op",
            "extra": "204820 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph - B/op",
            "value": 26536,
            "unit": "B/op",
            "extra": "204820 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph - allocs/op",
            "value": 241,
            "unit": "allocs/op",
            "extra": "204820 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph",
            "value": 27936,
            "unit": "ns/op\t   22952 B/op\t     281 allocs/op",
            "extra": "216924 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph - ns/op",
            "value": 27936,
            "unit": "ns/op",
            "extra": "216924 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph - B/op",
            "value": 22952,
            "unit": "B/op",
            "extra": "216924 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph - allocs/op",
            "value": 281,
            "unit": "allocs/op",
            "extra": "216924 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph",
            "value": 2152,
            "unit": "ns/op\t    1368 B/op\t      32 allocs/op",
            "extra": "2778312 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph - ns/op",
            "value": 2152,
            "unit": "ns/op",
            "extra": "2778312 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph - B/op",
            "value": 1368,
            "unit": "B/op",
            "extra": "2778312 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph - allocs/op",
            "value": 32,
            "unit": "allocs/op",
            "extra": "2778312 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph",
            "value": 35868,
            "unit": "ns/op\t   39505 B/op\t     228 allocs/op",
            "extra": "167461 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph - ns/op",
            "value": 35868,
            "unit": "ns/op",
            "extra": "167461 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph - B/op",
            "value": 39505,
            "unit": "B/op",
            "extra": "167461 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph - allocs/op",
            "value": 228,
            "unit": "allocs/op",
            "extra": "167461 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph",
            "value": 8488,
            "unit": "ns/op\t    6800 B/op\t      99 allocs/op",
            "extra": "709623 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph - ns/op",
            "value": 8488,
            "unit": "ns/op",
            "extra": "709623 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph - B/op",
            "value": 6800,
            "unit": "B/op",
            "extra": "709623 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph - allocs/op",
            "value": 99,
            "unit": "allocs/op",
            "extra": "709623 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources",
            "value": 9931,
            "unit": "ns/op\t    7656 B/op\t     123 allocs/op",
            "extra": "598077 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources - ns/op",
            "value": 9931,
            "unit": "ns/op",
            "extra": "598077 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources - B/op",
            "value": 7656,
            "unit": "B/op",
            "extra": "598077 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources - allocs/op",
            "value": 123,
            "unit": "allocs/op",
            "extra": "598077 times"
          },
          {
            "name": "BenchmarkRelationsReferencing",
            "value": 18.41,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "00000325236394 times"
          },
          {
            "name": "BenchmarkRelationsReferencing - ns/op",
            "value": 18.41,
            "unit": "ns/op",
            "extra": "00000325236394 times"
          },
          {
            "name": "BenchmarkRelationsReferencing - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "00000325236394 times"
          },
          {
            "name": "BenchmarkRelationsReferencing - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "00000325236394 times"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "tstirrat@gmail.com",
            "name": "Tanner Stirrat",
            "username": "tstirrat15"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "3d988010598224c10e2860efc8822a861f3a21a0",
          "message": "ci: use arm runners in integration tests (#2877)",
          "timestamp": "2026-02-04T10:36:16-07:00",
          "tree_id": "571e85a106173ac228f2e1a69267983e09e8720f",
          "url": "https://github.com/authzed/spicedb/commit/3d988010598224c10e2860efc8822a861f3a21a0"
        },
        "date": 1770227337744,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead",
            "value": 384170,
            "unit": "ns/op\t   15340 B/op\t     221 allocs/op",
            "extra": "15486 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead - ns/op",
            "value": 384170,
            "unit": "ns/op",
            "extra": "15486 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead - B/op",
            "value": 15340,
            "unit": "B/op",
            "extra": "15486 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotRead - allocs/op",
            "value": 221,
            "unit": "allocs/op",
            "extra": "15486 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace",
            "value": 8162638,
            "unit": "ns/op\t   94270 B/op\t   20136 allocs/op",
            "extra": "735 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace - ns/op",
            "value": 8162638,
            "unit": "ns/op",
            "extra": "735 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace - B/op",
            "value": 94270,
            "unit": "B/op",
            "extra": "735 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReadOnlyNamespace - allocs/op",
            "value": 20136,
            "unit": "allocs/op",
            "extra": "735 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource",
            "value": 8192051,
            "unit": "ns/op\t   97358 B/op\t   20194 allocs/op",
            "extra": "729 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - ns/op",
            "value": 8192051,
            "unit": "ns/op",
            "extra": "729 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - B/op",
            "value": 97358,
            "unit": "B/op",
            "extra": "729 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "729 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject",
            "value": 10238942,
            "unit": "ns/op\t   97362 B/op\t   20194 allocs/op",
            "extra": "584 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - ns/op",
            "value": 10238942,
            "unit": "ns/op",
            "extra": "584 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - B/op",
            "value": 97362,
            "unit": "B/op",
            "extra": "584 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "584 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource",
            "value": 7991590,
            "unit": "ns/op\t   77781 B/op\t   15209 allocs/op",
            "extra": "746 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource - ns/op",
            "value": 7991590,
            "unit": "ns/op",
            "extra": "746 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource - B/op",
            "value": 77781,
            "unit": "B/op",
            "extra": "746 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/ByResource - allocs/op",
            "value": 15209,
            "unit": "allocs/op",
            "extra": "746 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject",
            "value": 9992315,
            "unit": "ns/op\t   77843 B/op\t   15210 allocs/op",
            "extra": "598 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject - ns/op",
            "value": 9992315,
            "unit": "ns/op",
            "extra": "598 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject - B/op",
            "value": 77843,
            "unit": "B/op",
            "extra": "598 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadWithRelation/BySubject - allocs/op",
            "value": 15210,
            "unit": "allocs/op",
            "extra": "598 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource",
            "value": 544644,
            "unit": "ns/op\t   18431 B/op\t     280 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - ns/op",
            "value": 544644,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - B/op",
            "value": 18431,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject",
            "value": 551383,
            "unit": "ns/op\t   18429 B/op\t     280 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - ns/op",
            "value": 551383,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - B/op",
            "value": 18429,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead",
            "value": 7817984,
            "unit": "ns/op\t  172705 B/op\t   20202 allocs/op",
            "extra": "766 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead - ns/op",
            "value": 7817984,
            "unit": "ns/op",
            "extra": "766 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead - B/op",
            "value": 172705,
            "unit": "B/op",
            "extra": "766 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/SnapshotReverseRead - allocs/op",
            "value": 20202,
            "unit": "allocs/op",
            "extra": "766 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch",
            "value": 16914347,
            "unit": "ns/op\t   23473 B/op\t     297 allocs/op",
            "extra": "357 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch - ns/op",
            "value": 16914347,
            "unit": "ns/op",
            "extra": "357 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch - B/op",
            "value": 23473,
            "unit": "B/op",
            "extra": "357 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Touch - allocs/op",
            "value": 297,
            "unit": "allocs/op",
            "extra": "357 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create",
            "value": 17595972,
            "unit": "ns/op\t   20880 B/op\t     292 allocs/op",
            "extra": "368 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create - ns/op",
            "value": 17595972,
            "unit": "ns/op",
            "extra": "368 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create - B/op",
            "value": 20880,
            "unit": "B/op",
            "extra": "368 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/Create - allocs/op",
            "value": 292,
            "unit": "allocs/op",
            "extra": "368 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_",
            "value": 24594473,
            "unit": "ns/op\t 4465756 B/op\t   45848 allocs/op",
            "extra": "235 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_ - ns/op",
            "value": 24594473,
            "unit": "ns/op",
            "extra": "235 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_ - B/op",
            "value": 4465756,
            "unit": "B/op",
            "extra": "235 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0_ - allocs/op",
            "value": 45848,
            "unit": "allocs/op",
            "extra": "235 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_",
            "value": 34276222,
            "unit": "ns/op\t 4512622 B/op\t   45985 allocs/op",
            "extra": "180 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_ - ns/op",
            "value": 34276222,
            "unit": "ns/op",
            "extra": "180 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_ - B/op",
            "value": 4512622,
            "unit": "B/op",
            "extra": "180 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.1_ - allocs/op",
            "value": 45985,
            "unit": "allocs/op",
            "extra": "180 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_",
            "value": 38243693,
            "unit": "ns/op\t 4442862 B/op\t   46443 allocs/op",
            "extra": "139 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_ - ns/op",
            "value": 38243693,
            "unit": "ns/op",
            "extra": "139 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_ - B/op",
            "value": 4442862,
            "unit": "B/op",
            "extra": "139 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.25_ - allocs/op",
            "value": 46443,
            "unit": "allocs/op",
            "extra": "139 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_",
            "value": 45730099,
            "unit": "ns/op\t 4655881 B/op\t   46957 allocs/op",
            "extra": "126 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_ - ns/op",
            "value": 45730099,
            "unit": "ns/op",
            "extra": "126 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_ - B/op",
            "value": 4655881,
            "unit": "B/op",
            "extra": "126 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/0.5_ - allocs/op",
            "value": 46957,
            "unit": "allocs/op",
            "extra": "126 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_",
            "value": 66214674,
            "unit": "ns/op\t 5173765 B/op\t   48111 allocs/op",
            "extra": "81 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_ - ns/op",
            "value": 66214674,
            "unit": "ns/op",
            "extra": "81 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_ - B/op",
            "value": 5173765,
            "unit": "B/op",
            "extra": "81 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-static/TestTuple/CreateAndTouch/1_ - allocs/op",
            "value": 48111,
            "unit": "allocs/op",
            "extra": "81 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead",
            "value": 378306,
            "unit": "ns/op\t   15341 B/op\t     221 allocs/op",
            "extra": "15757 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead - ns/op",
            "value": 378306,
            "unit": "ns/op",
            "extra": "15757 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead - B/op",
            "value": 15341,
            "unit": "B/op",
            "extra": "15757 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotRead - allocs/op",
            "value": 221,
            "unit": "allocs/op",
            "extra": "15757 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace",
            "value": 8138864,
            "unit": "ns/op\t   94252 B/op\t   20135 allocs/op",
            "extra": "733 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace - ns/op",
            "value": 8138864,
            "unit": "ns/op",
            "extra": "733 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace - B/op",
            "value": 94252,
            "unit": "B/op",
            "extra": "733 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReadOnlyNamespace - allocs/op",
            "value": 20135,
            "unit": "allocs/op",
            "extra": "733 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource",
            "value": 8158361,
            "unit": "ns/op\t   97340 B/op\t   20194 allocs/op",
            "extra": "735 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - ns/op",
            "value": 8158361,
            "unit": "ns/op",
            "extra": "735 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - B/op",
            "value": 97340,
            "unit": "B/op",
            "extra": "735 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/ByResource - allocs/op",
            "value": 20194,
            "unit": "allocs/op",
            "extra": "735 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject",
            "value": 10260526,
            "unit": "ns/op\t   97361 B/op\t   20195 allocs/op",
            "extra": "585 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - ns/op",
            "value": 10260526,
            "unit": "ns/op",
            "extra": "585 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - B/op",
            "value": 97361,
            "unit": "B/op",
            "extra": "585 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadOnlyNamespace/BySubject - allocs/op",
            "value": 20195,
            "unit": "allocs/op",
            "extra": "585 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource",
            "value": 7990712,
            "unit": "ns/op\t   77796 B/op\t   15209 allocs/op",
            "extra": "751 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource - ns/op",
            "value": 7990712,
            "unit": "ns/op",
            "extra": "751 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource - B/op",
            "value": 77796,
            "unit": "B/op",
            "extra": "751 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/ByResource - allocs/op",
            "value": 15209,
            "unit": "allocs/op",
            "extra": "751 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject",
            "value": 9933387,
            "unit": "ns/op\t   77818 B/op\t   15209 allocs/op",
            "extra": "600 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject - ns/op",
            "value": 9933387,
            "unit": "ns/op",
            "extra": "600 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject - B/op",
            "value": 77818,
            "unit": "B/op",
            "extra": "600 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadWithRelation/BySubject - allocs/op",
            "value": 15209,
            "unit": "allocs/op",
            "extra": "600 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject",
            "value": 527942,
            "unit": "ns/op\t   18429 B/op\t     280 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - ns/op",
            "value": 527942,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - B/op",
            "value": 18429,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/BySubject - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource",
            "value": 536112,
            "unit": "ns/op\t   18431 B/op\t     280 allocs/op",
            "extra": "9386 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - ns/op",
            "value": 536112,
            "unit": "ns/op",
            "extra": "9386 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - B/op",
            "value": 18431,
            "unit": "B/op",
            "extra": "9386 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SortedSnapshotReadAllResourceFields/ByResource - allocs/op",
            "value": 280,
            "unit": "allocs/op",
            "extra": "9386 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead",
            "value": 7834890,
            "unit": "ns/op\t  172724 B/op\t   20202 allocs/op",
            "extra": "763 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead - ns/op",
            "value": 7834890,
            "unit": "ns/op",
            "extra": "763 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead - B/op",
            "value": 172724,
            "unit": "B/op",
            "extra": "763 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/SnapshotReverseRead - allocs/op",
            "value": 20202,
            "unit": "allocs/op",
            "extra": "763 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch",
            "value": 2988343,
            "unit": "ns/op\t   20941 B/op\t     272 allocs/op",
            "extra": "1964 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch - ns/op",
            "value": 2988343,
            "unit": "ns/op",
            "extra": "1964 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch - B/op",
            "value": 20941,
            "unit": "B/op",
            "extra": "1964 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Touch - allocs/op",
            "value": 272,
            "unit": "allocs/op",
            "extra": "1964 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create",
            "value": 1679714,
            "unit": "ns/op\t   18299 B/op\t     266 allocs/op",
            "extra": "3759 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create - ns/op",
            "value": 1679714,
            "unit": "ns/op",
            "extra": "3759 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create - B/op",
            "value": 18299,
            "unit": "B/op",
            "extra": "3759 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/Create - allocs/op",
            "value": 266,
            "unit": "allocs/op",
            "extra": "3759 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_",
            "value": 16671538,
            "unit": "ns/op\t 4463288 B/op\t   45823 allocs/op",
            "extra": "351 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_ - ns/op",
            "value": 16671538,
            "unit": "ns/op",
            "extra": "351 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_ - B/op",
            "value": 4463288,
            "unit": "B/op",
            "extra": "351 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0_ - allocs/op",
            "value": 45823,
            "unit": "allocs/op",
            "extra": "351 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_",
            "value": 26258447,
            "unit": "ns/op\t 4509884 B/op\t   45959 allocs/op",
            "extra": "222 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_ - ns/op",
            "value": 26258447,
            "unit": "ns/op",
            "extra": "222 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_ - B/op",
            "value": 4509884,
            "unit": "B/op",
            "extra": "222 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.1_ - allocs/op",
            "value": 45959,
            "unit": "allocs/op",
            "extra": "222 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_",
            "value": 29966917,
            "unit": "ns/op\t 4440121 B/op\t   46420 allocs/op",
            "extra": "183 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_ - ns/op",
            "value": 29966917,
            "unit": "ns/op",
            "extra": "183 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_ - B/op",
            "value": 4440121,
            "unit": "B/op",
            "extra": "183 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.25_ - allocs/op",
            "value": 46420,
            "unit": "allocs/op",
            "extra": "183 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_",
            "value": 45036284,
            "unit": "ns/op\t 4653533 B/op\t   46932 allocs/op",
            "extra": "134 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_ - ns/op",
            "value": 45036284,
            "unit": "ns/op",
            "extra": "134 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_ - B/op",
            "value": 4653533,
            "unit": "B/op",
            "extra": "134 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/0.5_ - allocs/op",
            "value": 46932,
            "unit": "allocs/op",
            "extra": "134 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_",
            "value": 64442599,
            "unit": "ns/op\t 5171176 B/op\t   48085 allocs/op",
            "extra": "81 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_ - ns/op",
            "value": 64442599,
            "unit": "ns/op",
            "extra": "81 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_ - B/op",
            "value": 5171176,
            "unit": "B/op",
            "extra": "81 times"
          },
          {
            "name": "BenchmarkDatastoreDriver/cockroachdb-overlap-insecure/TestTuple/CreateAndTouch/1_ - allocs/op",
            "value": 48085,
            "unit": "allocs/op",
            "extra": "81 times"
          },
          {
            "name": "BenchmarkQueryRelationships",
            "value": 440001,
            "unit": "ns/op\t     696 B/op\t      18 allocs/op",
            "extra": "13734 times"
          },
          {
            "name": "BenchmarkQueryRelationships - ns/op",
            "value": 440001,
            "unit": "ns/op",
            "extra": "13734 times"
          },
          {
            "name": "BenchmarkQueryRelationships - B/op",
            "value": 696,
            "unit": "B/op",
            "extra": "13734 times"
          },
          {
            "name": "BenchmarkQueryRelationships - allocs/op",
            "value": 18,
            "unit": "allocs/op",
            "extra": "13734 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true",
            "value": 2937070,
            "unit": "ns/op\t 2245615 B/op\t   17080 allocs/op",
            "extra": "2094 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true - ns/op",
            "value": 2937070,
            "unit": "ns/op",
            "extra": "2094 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true - B/op",
            "value": 2245615,
            "unit": "B/op",
            "extra": "2094 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=true - allocs/op",
            "value": 17080,
            "unit": "allocs/op",
            "extra": "2094 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false",
            "value": 2458042,
            "unit": "ns/op\t 2101433 B/op\t   14076 allocs/op",
            "extra": "2466 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false - ns/op",
            "value": 2458042,
            "unit": "ns/op",
            "extra": "2466 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false - B/op",
            "value": 2101433,
            "unit": "B/op",
            "extra": "2466 times"
          },
          {
            "name": "BenchmarkQueryRelsWithIntegrity/withIntegrity=false - allocs/op",
            "value": 14076,
            "unit": "allocs/op",
            "extra": "2466 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash",
            "value": 624.8,
            "unit": "ns/op\t     144 B/op\t       3 allocs/op",
            "extra": "9578293 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash - ns/op",
            "value": 624.8,
            "unit": "ns/op",
            "extra": "9578293 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash - B/op",
            "value": 144,
            "unit": "B/op",
            "extra": "9578293 times"
          },
          {
            "name": "BenchmarkComputeRelationshipHash - allocs/op",
            "value": 3,
            "unit": "allocs/op",
            "extra": "9578293 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions",
            "value": 138.2,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "43303974 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions - ns/op",
            "value": 138.2,
            "unit": "ns/op",
            "extra": "43303974 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "43303974 times"
          },
          {
            "name": "BenchmarkOptimizedRevisions - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "43303974 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching",
            "value": 26646,
            "unit": "ns/op\t    3977 B/op\t      72 allocs/op",
            "extra": "307412 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching - ns/op",
            "value": 26646,
            "unit": "ns/op",
            "extra": "307412 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching - B/op",
            "value": 3977,
            "unit": "B/op",
            "extra": "307412 times"
          },
          {
            "name": "BenchmarkSecondaryDispatching - allocs/op",
            "value": 72,
            "unit": "allocs/op",
            "extra": "307412 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression",
            "value": 2068,
            "unit": "ns/op\t    1577 B/op\t      34 allocs/op",
            "extra": "2923632 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression - ns/op",
            "value": 2068,
            "unit": "ns/op",
            "extra": "2923632 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression - B/op",
            "value": 1577,
            "unit": "B/op",
            "extra": "2923632 times"
          },
          {
            "name": "BenchmarkRunDispatchExpression - allocs/op",
            "value": 34,
            "unit": "allocs/op",
            "extra": "2923632 times"
          },
          {
            "name": "BenchmarkPatternMatcher",
            "value": 3621,
            "unit": "ns/op\t    5952 B/op\t      50 allocs/op",
            "extra": "1655592 times"
          },
          {
            "name": "BenchmarkPatternMatcher - ns/op",
            "value": 3621,
            "unit": "ns/op",
            "extra": "1655592 times"
          },
          {
            "name": "BenchmarkPatternMatcher - B/op",
            "value": 5952,
            "unit": "B/op",
            "extra": "1655592 times"
          },
          {
            "name": "BenchmarkPatternMatcher - allocs/op",
            "value": 50,
            "unit": "allocs/op",
            "extra": "1655592 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1",
            "value": 3827047,
            "unit": "ns/op\t  297762 B/op\t    4481 allocs/op",
            "extra": "2142 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1 - ns/op",
            "value": 3827047,
            "unit": "ns/op",
            "extra": "2142 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1 - B/op",
            "value": 297762,
            "unit": "B/op",
            "extra": "2142 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 4481,
            "unit": "allocs/op",
            "extra": "2142 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1",
            "value": 2047938,
            "unit": "ns/op\t  307800 B/op\t    4660 allocs/op",
            "extra": "3547 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1 - ns/op",
            "value": 2047938,
            "unit": "ns/op",
            "extra": "3547 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1 - B/op",
            "value": 307800,
            "unit": "B/op",
            "extra": "3547 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/postgres/v1 - allocs/op",
            "value": 4660,
            "unit": "allocs/op",
            "extra": "3547 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1",
            "value": 902121,
            "unit": "ns/op\t  236553 B/op\t    3680 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1 - ns/op",
            "value": 902121,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1 - B/op",
            "value": 236553,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_lookup_of_view_for_a_user/memory/v1 - allocs/op",
            "value": 3680,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1",
            "value": 9524574,
            "unit": "ns/op\t  548835 B/op\t    7701 allocs/op",
            "extra": "838 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1 - ns/op",
            "value": 9524574,
            "unit": "ns/op",
            "extra": "838 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1 - B/op",
            "value": 548835,
            "unit": "B/op",
            "extra": "838 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/cockroachdb/v1 - allocs/op",
            "value": 7701,
            "unit": "allocs/op",
            "extra": "838 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1",
            "value": 4615674,
            "unit": "ns/op\t  588715 B/op\t    8319 allocs/op",
            "extra": "1551 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1 - ns/op",
            "value": 4615674,
            "unit": "ns/op",
            "extra": "1551 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1 - B/op",
            "value": 588715,
            "unit": "B/op",
            "extra": "1551 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/postgres/v1 - allocs/op",
            "value": 8319,
            "unit": "allocs/op",
            "extra": "1551 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1",
            "value": 1302792,
            "unit": "ns/op\t  331136 B/op\t    4693 allocs/op",
            "extra": "7504 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1 - ns/op",
            "value": 1302792,
            "unit": "ns/op",
            "extra": "7504 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1 - B/op",
            "value": 331136,
            "unit": "B/op",
            "extra": "7504 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_groups/memory/v1 - allocs/op",
            "value": 4693,
            "unit": "allocs/op",
            "extra": "7504 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1",
            "value": 18529846,
            "unit": "ns/op\t 1891443 B/op\t   24244 allocs/op",
            "extra": "423 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1 - ns/op",
            "value": 18529846,
            "unit": "ns/op",
            "extra": "423 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1 - B/op",
            "value": 1891443,
            "unit": "B/op",
            "extra": "423 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/cockroachdb/v1 - allocs/op",
            "value": 24244,
            "unit": "allocs/op",
            "extra": "423 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1",
            "value": 11627231,
            "unit": "ns/op\t 1986541 B/op\t   25497 allocs/op",
            "extra": "711 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1 - ns/op",
            "value": 11627231,
            "unit": "ns/op",
            "extra": "711 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1 - B/op",
            "value": 1986541,
            "unit": "B/op",
            "extra": "711 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/postgres/v1 - allocs/op",
            "value": 25497,
            "unit": "allocs/op",
            "extra": "711 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1",
            "value": 5819414,
            "unit": "ns/op\t 1279858 B/op\t   14042 allocs/op",
            "extra": "1837 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1 - ns/op",
            "value": 5819414,
            "unit": "ns/op",
            "extra": "1837 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1 - B/op",
            "value": 1279858,
            "unit": "B/op",
            "extra": "1837 times"
          },
          {
            "name": "BenchmarkServices/recursively_through_wide_groups/memory/v1 - allocs/op",
            "value": 14042,
            "unit": "allocs/op",
            "extra": "1837 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1",
            "value": 41054355,
            "unit": "ns/op\t 9376313 B/op\t  127369 allocs/op",
            "extra": "223 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1 - ns/op",
            "value": 41054355,
            "unit": "ns/op",
            "extra": "223 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1 - B/op",
            "value": 9376313,
            "unit": "B/op",
            "extra": "223 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/cockroachdb/v1 - allocs/op",
            "value": 127369,
            "unit": "allocs/op",
            "extra": "223 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1",
            "value": 34592798,
            "unit": "ns/op\t 9401276 B/op\t  129894 allocs/op",
            "extra": "174 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1 - ns/op",
            "value": 34592798,
            "unit": "ns/op",
            "extra": "174 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1 - B/op",
            "value": 9401276,
            "unit": "B/op",
            "extra": "174 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/postgres/v1 - allocs/op",
            "value": 129894,
            "unit": "allocs/op",
            "extra": "174 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1",
            "value": 31114128,
            "unit": "ns/op\t 9293195 B/op\t  126314 allocs/op",
            "extra": "190 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1 - ns/op",
            "value": 31114128,
            "unit": "ns/op",
            "extra": "190 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1 - B/op",
            "value": 9293195,
            "unit": "B/op",
            "extra": "190 times"
          },
          {
            "name": "BenchmarkServices/lookup_with_intersection/memory/v1 - allocs/op",
            "value": 126314,
            "unit": "allocs/op",
            "extra": "190 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1",
            "value": 4743511,
            "unit": "ns/op\t  266135 B/op\t    4057 allocs/op",
            "extra": "1729 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1 - ns/op",
            "value": 4743511,
            "unit": "ns/op",
            "extra": "1729 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1 - B/op",
            "value": 266135,
            "unit": "B/op",
            "extra": "1729 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 4057,
            "unit": "allocs/op",
            "extra": "1729 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1",
            "value": 1936087,
            "unit": "ns/op\t  280588 B/op\t    4315 allocs/op",
            "extra": "3615 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1 - ns/op",
            "value": 1936087,
            "unit": "ns/op",
            "extra": "3615 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1 - B/op",
            "value": 280588,
            "unit": "B/op",
            "extra": "3615 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/postgres/v1 - allocs/op",
            "value": 4315,
            "unit": "allocs/op",
            "extra": "3615 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1",
            "value": 570567,
            "unit": "ns/op\t  201910 B/op\t    3367 allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1 - ns/op",
            "value": 570567,
            "unit": "ns/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1 - B/op",
            "value": 201910,
            "unit": "B/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/basic_check_for_a_user/memory/v1 - allocs/op",
            "value": 3367,
            "unit": "allocs/op",
            "extra": "10000 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1",
            "value": 11288460,
            "unit": "ns/op\t  861482 B/op\t   13491 allocs/op",
            "extra": "649 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1 - ns/op",
            "value": 11288460,
            "unit": "ns/op",
            "extra": "649 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1 - B/op",
            "value": 861482,
            "unit": "B/op",
            "extra": "649 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 13491,
            "unit": "allocs/op",
            "extra": "649 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1",
            "value": 4369905,
            "unit": "ns/op\t  915831 B/op\t   14462 allocs/op",
            "extra": "1303 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1 - ns/op",
            "value": 4369905,
            "unit": "ns/op",
            "extra": "1303 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1 - B/op",
            "value": 915831,
            "unit": "B/op",
            "extra": "1303 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/postgres/v1 - allocs/op",
            "value": 14462,
            "unit": "allocs/op",
            "extra": "1303 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1",
            "value": 1355201,
            "unit": "ns/op\t  657780 B/op\t   11777 allocs/op",
            "extra": "4184 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1 - ns/op",
            "value": 1355201,
            "unit": "ns/op",
            "extra": "4184 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1 - B/op",
            "value": 657780,
            "unit": "B/op",
            "extra": "4184 times"
          },
          {
            "name": "BenchmarkServices/recursive_check_for_a_user/memory/v1 - allocs/op",
            "value": 11777,
            "unit": "allocs/op",
            "extra": "4184 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1",
            "value": 12019262,
            "unit": "ns/op\t  583032 B/op\t    7918 allocs/op",
            "extra": "510 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1 - ns/op",
            "value": 12019262,
            "unit": "ns/op",
            "extra": "510 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1 - B/op",
            "value": 583032,
            "unit": "B/op",
            "extra": "510 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/cockroachdb/v1 - allocs/op",
            "value": 7918,
            "unit": "allocs/op",
            "extra": "510 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1",
            "value": 4013686,
            "unit": "ns/op\t  633320 B/op\t    8516 allocs/op",
            "extra": "1432 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1 - ns/op",
            "value": 4013686,
            "unit": "ns/op",
            "extra": "1432 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1 - B/op",
            "value": 633320,
            "unit": "B/op",
            "extra": "1432 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/postgres/v1 - allocs/op",
            "value": 8516,
            "unit": "allocs/op",
            "extra": "1432 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1",
            "value": 788343,
            "unit": "ns/op\t  344256 B/op\t    4317 allocs/op",
            "extra": "6925 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1 - ns/op",
            "value": 788343,
            "unit": "ns/op",
            "extra": "6925 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1 - B/op",
            "value": 344256,
            "unit": "B/op",
            "extra": "6925 times"
          },
          {
            "name": "BenchmarkServices/wide_groups_check_for_a_user/memory/v1 - allocs/op",
            "value": 4317,
            "unit": "allocs/op",
            "extra": "6925 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1",
            "value": 3655053,
            "unit": "ns/op\t  130780 B/op\t    2011 allocs/op",
            "extra": "2576 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1 - ns/op",
            "value": 3655053,
            "unit": "ns/op",
            "extra": "2576 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1 - B/op",
            "value": 130780,
            "unit": "B/op",
            "extra": "2576 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/cockroachdb/v1 - allocs/op",
            "value": 2011,
            "unit": "allocs/op",
            "extra": "2576 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1",
            "value": 2603111,
            "unit": "ns/op\t  137119 B/op\t    2105 allocs/op",
            "extra": "2332 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1 - ns/op",
            "value": 2603111,
            "unit": "ns/op",
            "extra": "2332 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1 - B/op",
            "value": 137119,
            "unit": "B/op",
            "extra": "2332 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/postgres/v1 - allocs/op",
            "value": 2105,
            "unit": "allocs/op",
            "extra": "2332 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1",
            "value": 224716,
            "unit": "ns/op\t   85813 B/op\t    1386 allocs/op",
            "extra": "26673 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1 - ns/op",
            "value": 224716,
            "unit": "ns/op",
            "extra": "26673 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1 - B/op",
            "value": 85813,
            "unit": "B/op",
            "extra": "26673 times"
          },
          {
            "name": "BenchmarkServices/wide_direct_relation_check/memory/v1 - allocs/op",
            "value": 1386,
            "unit": "allocs/op",
            "extra": "26673 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph",
            "value": 8689,
            "unit": "ns/op\t    6824 B/op\t      98 allocs/op",
            "extra": "712987 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph - ns/op",
            "value": 8689,
            "unit": "ns/op",
            "extra": "712987 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph - B/op",
            "value": 6824,
            "unit": "B/op",
            "extra": "712987 times"
          },
          {
            "name": "BenchmarkRecursiveShallowGraph - allocs/op",
            "value": 98,
            "unit": "allocs/op",
            "extra": "712987 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph",
            "value": 30236,
            "unit": "ns/op\t   26536 B/op\t     241 allocs/op",
            "extra": "199262 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph - ns/op",
            "value": 30236,
            "unit": "ns/op",
            "extra": "199262 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph - B/op",
            "value": 26536,
            "unit": "B/op",
            "extra": "199262 times"
          },
          {
            "name": "BenchmarkRecursiveWideGraph - allocs/op",
            "value": 241,
            "unit": "allocs/op",
            "extra": "199262 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph",
            "value": 29113,
            "unit": "ns/op\t   22952 B/op\t     281 allocs/op",
            "extra": "205707 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph - ns/op",
            "value": 29113,
            "unit": "ns/op",
            "extra": "205707 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph - B/op",
            "value": 22952,
            "unit": "B/op",
            "extra": "205707 times"
          },
          {
            "name": "BenchmarkRecursiveDeepGraph - allocs/op",
            "value": 281,
            "unit": "allocs/op",
            "extra": "205707 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph",
            "value": 2201,
            "unit": "ns/op\t    1368 B/op\t      32 allocs/op",
            "extra": "2721835 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph - ns/op",
            "value": 2201,
            "unit": "ns/op",
            "extra": "2721835 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph - B/op",
            "value": 1368,
            "unit": "B/op",
            "extra": "2721835 times"
          },
          {
            "name": "BenchmarkRecursiveEmptyGraph - allocs/op",
            "value": 32,
            "unit": "allocs/op",
            "extra": "2721835 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph",
            "value": 37297,
            "unit": "ns/op\t   39505 B/op\t     228 allocs/op",
            "extra": "160578 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph - ns/op",
            "value": 37297,
            "unit": "ns/op",
            "extra": "160578 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph - B/op",
            "value": 39505,
            "unit": "B/op",
            "extra": "160578 times"
          },
          {
            "name": "BenchmarkRecursiveSparseGraph - allocs/op",
            "value": 228,
            "unit": "allocs/op",
            "extra": "160578 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph",
            "value": 8806,
            "unit": "ns/op\t    6800 B/op\t      99 allocs/op",
            "extra": "681878 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph - ns/op",
            "value": 8806,
            "unit": "ns/op",
            "extra": "681878 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph - B/op",
            "value": 6800,
            "unit": "B/op",
            "extra": "681878 times"
          },
          {
            "name": "BenchmarkRecursiveCyclicGraph - allocs/op",
            "value": 99,
            "unit": "allocs/op",
            "extra": "681878 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources",
            "value": 10343,
            "unit": "ns/op\t    7656 B/op\t     123 allocs/op",
            "extra": "583641 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources - ns/op",
            "value": 10343,
            "unit": "ns/op",
            "extra": "583641 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources - B/op",
            "value": 7656,
            "unit": "B/op",
            "extra": "583641 times"
          },
          {
            "name": "BenchmarkRecursiveIterResources - allocs/op",
            "value": 123,
            "unit": "allocs/op",
            "extra": "583641 times"
          },
          {
            "name": "BenchmarkRelationsReferencing",
            "value": 18.37,
            "unit": "ns/op\t       0 B/op\t       0 allocs/op",
            "extra": "00000327030218 times"
          },
          {
            "name": "BenchmarkRelationsReferencing - ns/op",
            "value": 18.37,
            "unit": "ns/op",
            "extra": "00000327030218 times"
          },
          {
            "name": "BenchmarkRelationsReferencing - B/op",
            "value": 0,
            "unit": "B/op",
            "extra": "00000327030218 times"
          },
          {
            "name": "BenchmarkRelationsReferencing - allocs/op",
            "value": 0,
            "unit": "allocs/op",
            "extra": "00000327030218 times"
          }
        ]
      }
    ]
  }
}