package newenemy

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"testing"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/authzed/grpcutil"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/pkg/zookie"
	"github.com/jackc/pgx/v4"
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
	cancel                   context.CancelFunc
)

func TestMain(m *testing.M) {
	testCtx, cancel = context.WithCancel(context.Background())
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	defer func() {
		signal.Stop(signalChan)
		cancel()
	}()
	go func() {
		select {
		case <-signalChan:
			cancel()
		case <-testCtx.Done():
		}
		<-signalChan
		os.Exit(2)
	}()

	rand.Seed(time.Now().UnixNano())

	grpcConn1, err := grpc.Dial("localhost:50051", grpc.WithInsecure(), grpcutil.WithInsecureBearerToken(token))
	if err != nil {
		log.Fatalf("unable to connect: %s", err)
	}
	grpcConn2, err := grpc.Dial("localhost:50052", grpc.WithInsecure(), grpcutil.WithInsecureBearerToken(token))
	if err != nil {
		log.Fatalf("unable to connect: %s", err)
	}
	grpcConn3, err := grpc.Dial("localhost:50055", grpc.WithInsecure(), grpcutil.WithInsecureBearerToken(token))
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

func TestNoNewEnemy(t *testing.T) {
	fillerCount := 2000
	tupleCount := fillerCount + 300
	batchSize := 100
	excludes, directs := generateTuples(tupleCount)

	// fill with tuples to ensure we span multiple ranges
	fill(fillerCount, batchSize, excludes, directs)

	// the transactions above probably got the timestamp caches in sync
	// sleep here so that we have a better chance
	time.Sleep(500 * time.Millisecond)

	candidates := 0
	for i := fillerCount; i < tupleCount; i++ {
		// write to node 2 (clock is behind)
		r1, err := client2.Write(testCtx, &v0.WriteRequest{
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
		} else {
			if canHas.Membership == v0.CheckResponse_MEMBER {
				fmt.Println("service is subject to the new enemy problem")
			}
		}

		analyzeCalls(excludes[i].Tuple, directs[i].Tuple, r1.GetRevision(), r2.GetRevision(), &candidates)

		// if we find 3 causal reversals, but no newenemy, assume we're protected
		if candidates > 3 {
			fmt.Println("3 (would be) causal reversals with no new enemy detected")
			return
		}
		if canHas.Membership == v0.CheckResponse_MEMBER {
			log.Fatalf("done")
		}
	}
	if candidates < 3 {
		log.Fatalf("didn't see 3 candidates, only saw %d", candidates)
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
	l1, l2 := getLeaderNode(testCtx, conn, t1), getLeaderNode(testCtx, conn, t2)
	if l1 != l2 {
		fmt.Println("different leaders for writes: ", l1, l2)
	}

	z1, _ := zookie.DecodeRevision(r1)
	z2, _ := zookie.DecodeRevision(r2)

	// this handles "sleep" candidates
	// if the direct write has an earlier timestamp than the exclude write,
	// we would expect the check to fail
	if z2.LessThan(z1) {
		*candidates++
		fmt.Println("candidate found")
	}

	// this handles "overlapping transaction" candidates
	// if the writes differ only the logical component,
	// then the transactions overlapped and crdb assigned them the correct order
	if z1.LessThan(z2) && z1.IntPart() == z2.IntPart() {
		*candidates++
		fmt.Println("candidate found")
	}
	fmt.Println(z1, z2, z1.Sub(z2).String())
}

// getLeaderNode returns the node with the lease leader for the range containing the tuple
func getLeaderNode(ctx context.Context, conn *pgx.Conn, tuple *v0.RelationTuple) byte {
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
	for rows.Next() {
		raw = rows.RawValues()[3]
		break
	}
	return raw[len(raw)-1]
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
