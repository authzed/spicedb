package newenemy

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"testing"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/authzed/grpcutil"
	"github.com/jackc/pgx/v4"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/e2e"
	"github.com/authzed/spicedb/pkg/zookie"
)

const schema = `
definition user {}
definition resource {
	relation direct: user
	relation excluded: user
	permission allowed = direct - excluded
}
`

const token = "testtesttesttest"

var (
	client, client2, client3 v0.ACLServiceClient
	schemaClient             v1alpha1.SchemaServiceClient
	conn                     *pgx.Conn
	testCtx                  context.Context
)

func TestMain(m *testing.M) {
	var cancel context.CancelFunc
	testCtx, cancel = context.WithCancel(e2e.Context())
	defer cancel()

	fmt.Println("starting cockroach...")
	if _, err := e2e.GoRun(testCtx, "crdb1.log",
		"./cockroach", "start", "--store=node1", "--logtostderr",
		"--insecure", "--listen-addr=localhost:26257",
		"--http-addr=localhost:8080",
		"--join=localhost:26257,localhost:26258,localhost:26259",
	); err != nil {
		log.Fatalf("unable to start crdb: %v", err)
	}
	pid, err := e2e.GoRun(testCtx, "crdb2.log",
		"./cockroach", "start", "--store=node2", "--logtostderr",
		"--insecure", "--listen-addr=localhost:26258",
		"--http-addr=localhost:8081",
		"--join=localhost:26257,localhost:26258,localhost:26259",
	)
	if err != nil {
		log.Fatalf("unable to start crdb: %v", err)
	}
	if _, err := e2e.GoRun(testCtx, "crdb3.log",
		"./cockroach", "start", "--store=node3", "--logtostderr",
		"--insecure", "--listen-addr=localhost:26259",
		"--http-addr=localhost:8082",
		"--join=localhost:26257,localhost:26258,localhost:26259",
	); err != nil {
		log.Fatalf("unable to start crdb: %v", err)
	}

	fmt.Println("initializing crdb...")

	// this auto-retries until it succeeds
	e2e.Run(testCtx, "", "./cockroach", "init", "--insecure", "--host=localhost:26257")

	if err := e2e.Run(testCtx, "",
		"./cockroach", "sql", "--insecure", "--host=localhost:26257",
		"-e", "CREATE DATABASE spicedb;",
	); err != nil {
		log.Fatalf("couldn't create db: %v", err)
	}
	if err := e2e.Run(testCtx, "",
		"./cockroach", "sql", "--insecure", "--host=localhost:26257",
		"-e", "ALTER DATABASE spicedb CONFIGURE ZONE USING range_min_bytes = 0, range_max_bytes = 65536, num_replicas = 1;",
	); err != nil {
		log.Fatalf("couldn't configure db: %v", err)
	}

	fmt.Println("fetching dependencies...")
	if err := e2e.Run(testCtx, "",
		"go", "get", "-d", "github.com/authzed/spicedb/cmd/spicedb/...",
	); err != nil {
		log.Fatalf("error fetching dependencies: %v", err)
	}

	fmt.Println("building...")
	if err := e2e.Run(testCtx, "",
		"go", "build", "github.com/authzed/spicedb/cmd/spicedb/...",
	); err != nil {
		log.Fatalf("error building spicedb: %v", err)
	}

	fmt.Println("migrating...")
	migrated := false
	for i := 0; i < 5; i++ {
		if err := e2e.Run(testCtx, "",
			"./spicedb",
			"migrate", "head", "--datastore-engine=cockroachdb",
			"--datastore-conn-uri=postgresql://root@localhost:26257/spicedb?sslmode=disable",
		); err == nil {
			migrated = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	if !migrated {
		log.Fatalf("couldn't migrate")
	}

	fmt.Println("starting spicedbs...")
	if _, err := e2e.GoRun(testCtx, "spicedb1.log",
		"./spicedb",
		"serve", "--log-level=debug", "--grpc-preshared-key="+token, "--grpc-no-tls",
		"--dashboard-addr=:8087", "--datastore-engine=cockroachdb",
		"--datastore-conn-uri=postgresql://root@localhost:26257/spicedb?sslmode=disable",
	); err != nil {
		log.Fatalf("unable to start spicedb: %v", err)
	}

	if _, err := e2e.GoRun(testCtx, "spicedb2.log",
		"./spicedb",
		"serve", "--log-level=debug", "--grpc-preshared-key="+token, "--grpc-no-tls",
		"--datastore-engine=cockroachdb", "--grpc-addr=:50052",
		"--internal-grpc-addr=:50054", "--metrics-addr=:9091", "--dashboard-addr=:8085",
		"--datastore-conn-uri=postgresql://root@localhost:26258/spicedb?sslmode=disable",
	); err != nil {
		log.Fatalf("unable to start spicedb: %v", err)
	}

	if _, err := e2e.GoRun(testCtx, "spicedb3.log",
		"./spicedb",
		"serve", "--log-level=debug", "--grpc-preshared-key="+token, "--grpc-no-tls",
		"--datastore-engine=cockroachdb", "--grpc-addr=:50055",
		"--internal-grpc-addr=:50056", "--metrics-addr=:9092", "--dashboard-addr=:8086",
		"--datastore-conn-uri=postgresql://root@localhost:26259/spicedb?sslmode=disable",
	); err != nil {
		log.Fatalf("unable to start spicedb: %v", err)
	}

	fmt.Println("modifying network")
	if err := e2e.Run(testCtx, "netattack.log",
		"sudo", "./chaosd", "attack", "network", "delay", "-l=100ms",
		"-e=26757", "-d=lo", "-p=tcp",
	); err != nil {
		log.Fatalf("unable to delay network: %v", err)
	}

	fmt.Println("modifying network")
	if err := e2e.Run(testCtx, "netattack.log",
		"sudo", "./chaosd", "attack", "network", "delay", "-l=100ms",
		"-e=26757", "-d=lo", "-p=tcp",
	); err != nil {
		log.Fatalf("unable to delay network: %v", err)
	}

	fmt.Println("modifying time")
	sec, nsec := secAndNSecFromDuration(-200 * time.Millisecond)
	if err := e2e.Run(testCtx, "timeattack.log",
		"sudo", "./watchmaker", fmt.Sprintf("--pid=%d", pid), fmt.Sprintf("--sec_delta=%d", sec),
		fmt.Sprintf("--nsec_delta=%d", nsec), "--clk_ids=CLOCK_REALTIME,CLOCK_MONOTONIC",
	); err != nil {
		log.Fatalf("unable to modify time: %v", err)
	}

	rand.Seed(time.Now().UnixNano())

	for {
		if func() bool {
			timeout := time.Second
			conn, err := net.DialTimeout("tcp", net.JoinHostPort("localhost", "50051"), timeout)
			if err != nil {
				fmt.Println("waiting for spicedbs to be ready:", err)
				time.Sleep(1 * time.Second)
				return false
			}
			if conn != nil {
				conn.Close()
			}
			return true
		}() {
			break
		}
	}

	fmt.Println("attempting to connect...")
	grpcConn1, err := grpc.DialContext(testCtx, "localhost:50051",
		grpc.WithBlock(), grpc.WithInsecure(),
		grpcutil.WithInsecureBearerToken(token))
	if err != nil {
		log.Fatalf("unable to connect: %s", err)
	}
	grpcConn2, err := grpc.DialContext(testCtx, "localhost:50052",
		grpc.WithBlock(), grpc.WithInsecure(),
		grpcutil.WithInsecureBearerToken(token))
	if err != nil {
		log.Fatalf("unable to connect: %s", err)
	}
	grpcConn3, err := grpc.DialContext(testCtx, "localhost:50055",
		grpc.WithBlock(), grpc.WithInsecure(), grpcutil.WithInsecureBearerToken(token))
	if err != nil {
		log.Fatalf("unable to connect: %s", err)
	}

	client = v0.NewACLServiceClient(grpcConn1)
	client2 = v0.NewACLServiceClient(grpcConn2)
	client3 = v0.NewACLServiceClient(grpcConn3)
	schemaClient = v1alpha1.NewSchemaServiceClient(grpcConn1)

	conn, err = pgx.Connect(testCtx, "postgresql://root@localhost:26259/spicedb?sslmode=disable")
	if err != nil {
		log.Fatal("failed to connect")
	}
	initSchema(schemaClient)
	m.Run()
}

func secAndNSecFromDuration(duration time.Duration) (sec int64, nsec int64) {
	sec = duration.Nanoseconds() / 1e9
	nsec = duration.Nanoseconds() - (sec * 1e9)

	return
}

func TestNoNewEnemy(t *testing.T) {
	fillerCount := 2000
	testCount := 1000
	batchSize := 100
	directs, excludes := generateTuples(fillerCount + testCount)

	// fill with tuples to ensure we span multiple ranges
	fill(fillerCount, batchSize, excludes, directs)

	// the transactions above probably got the timestamp caches in sync
	// sleep here so that we have a better chance
	time.Sleep(500 * time.Millisecond)

	candidates := 0
	for i := fillerCount; i < fillerCount+testCount; i++ {
		// write to node 1
		r1, err := client.Write(testCtx, &v0.WriteRequest{
			Updates: []*v0.RelationTupleUpdate{excludes[i]},
		})
		if err != nil {
			fmt.Println(err)
			continue
		}

		// write to node 2 (clock is behind)
		r2, err := client2.Write(testCtx, &v0.WriteRequest{
			Updates: []*v0.RelationTupleUpdate{directs[i]},
		})
		if err != nil {
			fmt.Println(err)
			continue
		}

		canHas, err := client3.Check(context.Background(), &v0.CheckRequest{
			TestUserset: &v0.ObjectAndRelation{
				Namespace: "resource",
				ObjectId:  "thegoods",
				Relation:  "allowed",
			},
			User:       directs[i].Tuple.GetUser(),
			AtRevision: r2.GetRevision(),
		})
		if err != nil {
			fmt.Println(err)
			continue
		}
		if canHas.IsMember {
			fmt.Println("service is subject to the new enemy problem")
		}

		analyzeCalls(excludes[i].Tuple, directs[i].Tuple, r1.GetRevision(), r2.GetRevision(), &candidates)

		if canHas.IsMember {
			log.Fatalf("done")
		}

		// if we find causal reversals, but no newenemy, assume we're protected
		if candidates > 5 {
			fmt.Println("5 (would be) causal reversals with no new enemy detected")
			return
		}

	}
	if candidates < 5 {
		log.Fatalf("didn't see 5 candidates, only saw %d", candidates)
	}
}

func BenchmarkBatchWrites(b *testing.B) {
	exludes, directs := generateTuples(b.N * 20)
	b.ResetTimer()
	_, err := client.Write(testCtx, &v0.WriteRequest{
		Updates: exludes,
	})
	if err != nil {
		fmt.Println(err)
	}
	_, err = client.Write(testCtx, &v0.WriteRequest{
		Updates: directs,
	})
	if err != nil {
		fmt.Println(err)
	}
}

func BenchmarkConflictingTupleWrites(b *testing.B) {
	fillerCount := 2000
	tupleCount := fillerCount + b.N*20
	batchSize := 100
	excludes, directs := generateTuples(tupleCount)

	// fill with tuples to ensure we span multiple ranges
	fill(fillerCount, batchSize, excludes, directs)

	b.ResetTimer()
	for i := fillerCount; i < tupleCount; i++ {
		// write to node 2 (clock is behind)
		_, err := client2.Write(testCtx, &v0.WriteRequest{
			Updates: []*v0.RelationTupleUpdate{excludes[i]},
		})
		if err != nil {
			fmt.Println(err)
			continue
		}

		// write to node 1
		r2, err := client.Write(testCtx, &v0.WriteRequest{
			Updates: []*v0.RelationTupleUpdate{directs[i]},
		})
		if err != nil {
			fmt.Println(err)
			continue
		}

		_, err = client3.Check(context.Background(), &v0.CheckRequest{
			TestUserset: &v0.ObjectAndRelation{
				Namespace: "resource",
				ObjectId:  "thegoods",
				Relation:  "allowed",
			},
			User:       directs[i].Tuple.GetUser(),
			AtRevision: r2.GetRevision(),
		})
		if err != nil {
			fmt.Println(err)
			continue
		}
	}
}

func fill(fillerCount, batchSize int, excludes, directs []*v0.RelationTupleUpdate) {
	for i := 0; i < fillerCount/batchSize; i++ {
		fmt.Println("filling ", i*batchSize, "to", (i+1)*batchSize)
		_, err := client.Write(testCtx, &v0.WriteRequest{
			Updates: excludes[i*batchSize : (i+1)*batchSize],
		})
		if err != nil {
			fmt.Println(err)
		}
		_, err = client.Write(testCtx, &v0.WriteRequest{
			Updates: directs[i*batchSize : (i+1)*batchSize],
		})
		if err != nil {
			fmt.Println(err)
		}
	}
}

func initSchema(schemaClient v1alpha1.SchemaServiceClient) {
	_, err := schemaClient.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: schema,
	})
	if err != nil {
		log.Fatalf("unable to initialize schema: %s", err)
	}
}

func generateTuples(n int) (directs []*v0.RelationTupleUpdate, excludes []*v0.RelationTupleUpdate) {
	directs = make([]*v0.RelationTupleUpdate, 0, n)
	excludes = make([]*v0.RelationTupleUpdate, 0, n)
	for i := 0; i < n; i++ {
		user := &v0.User{
			UserOneof: &v0.User_Userset{
				Userset: &v0.ObjectAndRelation{
					Namespace: "user",
					ObjectId:  randSeq(16),
					Relation:  "...",
				},
			},
		}
		tupleExclude := &v0.RelationTuple{
			ObjectAndRelation: &v0.ObjectAndRelation{
				Namespace: "resource",
				ObjectId:  "thegoods",
				Relation:  "excluded",
			},
			User: user,
		}
		tupleDirect := &v0.RelationTuple{
			ObjectAndRelation: &v0.ObjectAndRelation{
				Namespace: "resource",
				ObjectId:  "thegoods",
				Relation:  "direct",
			},
			User: user,
		}
		excludes = append(excludes, &v0.RelationTupleUpdate{
			Operation: v0.RelationTupleUpdate_TOUCH,
			Tuple:     tupleExclude,
		})
		directs = append(directs, &v0.RelationTupleUpdate{
			Operation: v0.RelationTupleUpdate_TOUCH,
			Tuple:     tupleDirect,
		})
	}
	return
}

// after we've checked, analyze the previous calls
func analyzeCalls(t1, t2 *v0.RelationTuple, r1, r2 *v0.Zookie, candidates *int) {
	l1, l1rs := getLeaderNode(testCtx, conn, t1)
	l2, l2rs := getLeaderNode(testCtx, conn, t2)

	z1, _ := zookie.DecodeRevision(r1)
	z2, _ := zookie.DecodeRevision(r2)

	// the best we can do when mitigations are enabled is guess that timestamps
	// with the same nanosecond timestamp were protected
	if z2.GreaterThan(z1) && z2.IntPart() == z1.IntPart() {
		*candidates++
		fmt.Println("candidate found")
	}

	fmt.Println(z1, z2, z1.Sub(z2).String(), l1, l2, l1rs, l2rs)
}

// getLeaderNode returns the node with the lease leader for the range containing the tuple
func getLeaderNode(ctx context.Context, conn *pgx.Conn, tuple *v0.RelationTuple) (byte, []byte) {
	t := tuple

	rows, err := conn.Query(ctx, "SHOW RANGE FROM TABLE relation_tuple FOR ROW ($1::text,$2::text,$3::text,$4::text,$5::text,$6::text)",
		t.ObjectAndRelation.Namespace,
		t.ObjectAndRelation.ObjectId,
		t.ObjectAndRelation.Relation,
		t.User.GetUserset().Namespace,
		t.User.GetUserset().ObjectId,
		t.User.GetUserset().Relation,
	)
	defer rows.Close()
	if err != nil {
		log.Fatalf("failed to exec: %v", err)
	}
	var raw []byte
	replicas := []byte{}
	for rows.Next() {
		// NOTE: this is in lieu of pulling in roachpb, it may be out of date
		// if those APIs change
		raw = rows.RawValues()[3]

		replicasRaw := rows.RawValues()[5]
		for {
			var found []byte
			replicasRaw, found = replicasRaw[:len(replicasRaw)-12], replicasRaw[len(replicasRaw)-12:]
			if found[3] == byte(8) {
				replicas = append(replicas, found[11])
			} else {
				break
			}
		}
		break
	}
	return raw[len(raw)-1], replicas
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
