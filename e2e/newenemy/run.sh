#!/usr/bin/env bash

set -e

# from https://aweirdimagination.net/2020/06/28/kill-child-jobs-on-script-exit/
cleanup() {
    # kill all processes whose parent is this process
    pkill -P $$
}

for sig in INT QUIT HUP TERM; do
  trap "
    cleanup
    trap - $sig EXIT
    kill -s $sig "'"$$"' "$sig"
done
trap cleanup EXIT

kind create cluster --config=kind.yaml
kind get clusters | head -1 | xargs -n1 -I{} kind export kubeconfig --name {}
./chaosmesh-install.sh --local kind
kubectl apply -f cockroach.yaml
kubectl rollout status --watch --timeout=600s statefulset/crdb
kubectl exec crdb-0 -- ./cockroach init --insecure --host=localhost:26257
kubectl exec crdb-0 -- ./cockroach sql --insecure --host=localhost:26257 -e "CREATE DATABASE spicedb;"
kubectl exec crdb-0 -- ./cockroach sql --insecure --host=localhost:26257 -e "ALTER DATABASE spicedb CONFIGURE ZONE USING range_min_bytes = 0, range_max_bytes = 65536, num_replicas = 1;"

echo "forwarding ports"
kubectl port-forward crdb-0 26257:26257 &
kubectl port-forward crdb-1 26258:26257 &
kubectl port-forward crdb-2 26259:26257 &

echo "migrating"
go get -d github.com/authzed/spicedb/cmd/spicedb/...
while ! go run github.com/authzed/spicedb/cmd/spicedb/... migrate head --datastore-engine=cockroachdb --datastore-conn-uri="postgresql://root@localhost:26257/spicedb?sslmode=disable"; do sleep 1; done

echo "starting spicedbs"
go run github.com/authzed/spicedb/cmd/spicedb/... --log-level=debug --grpc-preshared-key=testtesttesttest --grpc-no-tls --datastore-engine=cockroachdb --datastore-conn-uri="postgresql://root@localhost:26257/spicedb?sslmode=disable" &> node1.log &
go run github.com/authzed/spicedb/cmd/spicedb/... --log-level=debug --grpc-preshared-key=testtesttesttest --grpc-no-tls --datastore-engine=cockroachdb --datastore-conn-uri="postgresql://root@localhost:26258/spicedb?sslmode=disable" --grpc-addr=:50052 --internal-grpc-addr=:50054 --metrics-addr=:9091 &> node2.log &
go run github.com/authzed/spicedb/cmd/spicedb/... --log-level=debug --grpc-preshared-key=testtesttesttest --grpc-no-tls --datastore-engine=cockroachdb --datastore-conn-uri="postgresql://root@localhost:26259/spicedb?sslmode=disable" --grpc-addr=:50055 --internal-grpc-addr=:50056 --metrics-addr=:9092 &> node3.log &

if [[ $(uname -m) == *arm* ]]; then
  echo "skipping chaos on arm"
else
  echo "modifying time and network"
  kubectl apply -f chaos.yaml
fi

# todo: health check grpc
sleep 3

echo "running test"
go test -v ./...

# benchmark
# go test -run='^$' -bench=. -v ./...