window.BENCHMARK_DATA = {
  "lastUpdate": 1769731319833,
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
      }
    ]
  }
}