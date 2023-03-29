# Datastore Driver Benchmark

## To Run

`go test ./... -tags ci,docker -bench . -benchmem`

## Results on an Macbook Pro M1 Max w/64 GB

Note: MySQL is using `mysql:5` image which doesn't have an arm image.

```sh
goos: darwin
goarch: arm64
pkg: github.com/authzed/spicedb/internal/datastore/benchmark
BenchmarkDatastoreDriver/postgres/TestTuple/SnapshotRead-10                 	    2100	    532025 ns/op	   13948 B/op	     232 allocs/op
BenchmarkDatastoreDriver/postgres/TestTuple/Touch-10                        	     500	   2289054 ns/op	   16622 B/op	     269 allocs/op
BenchmarkDatastoreDriver/postgres/TestTuple/Create-10                       	     720	   1820216 ns/op	   10057 B/op	     141 allocs/op
BenchmarkDatastoreDriver/crdb-overlap-static/TestTuple/SnapshotRead-10         	    1357	    830114 ns/op	   13263 B/op	     211 allocs/op
BenchmarkDatastoreDriver/crdb-overlap-static/TestTuple/Touch-10                	     100	  11445458 ns/op	   16526 B/op	     225 allocs/op
BenchmarkDatastoreDriver/crdb-overlap-static/TestTuple/Create-10               	     100	  11145802 ns/op	   14908 B/op	     220 allocs/op
BenchmarkDatastoreDriver/crdb-overlap-insecure/TestTuple/SnapshotRead-10       	    1590	    698306 ns/op	   13269 B/op	     211 allocs/op
BenchmarkDatastoreDriver/crdb-overlap-insecure/TestTuple/Touch-10              	     364	   3566063 ns/op	   14613 B/op	     211 allocs/op
BenchmarkDatastoreDriver/crdb-overlap-insecure/TestTuple/Create-10             	     370	   2984686 ns/op	   13035 B/op	     206 allocs/op
BenchmarkDatastoreDriver/mysql/TestTuple/SnapshotRead-10                       	     477	   2483215 ns/op	   14355 B/op	     312 allocs/op
BenchmarkDatastoreDriver/mysql/TestTuple/Touch-10                              	     160	   7413188 ns/op	   11717 B/op	     207 allocs/op
BenchmarkDatastoreDriver/mysql/TestTuple/Create-10                             	     241	   4857547 ns/op	    6380 B/op	     113 allocs/op
BenchmarkDatastoreDriver/memdb/TestTuple/SnapshotRead-10                       	   12073	     98569 ns/op	    2375 B/op	      48 allocs/op
BenchmarkDatastoreDriver/memdb/TestTuple/Touch-10                              	   35023	     38734 ns/op	   44556 B/op	     522 allocs/op
BenchmarkDatastoreDriver/memdb/TestTuple/Create-10                             	   36390	     40246 ns/op	   48386 B/op	     528 allocs/op
```
