package ecs

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc/resolver"

	"github.com/authzed/spicedb/pkg/testutil"
)

const (
	testCluster = "my-cluster"
	testService = "spicedb"
	testPort    = "50053"
)

var (
	testTarget = "ecs:///" + testCluster + "/" + testService + ":" + testPort
	// Addresses are published in lexicographic order, so the self address is
	// deliberately not a prefix of any task address.
	testSelf = Self{TaskARN: taskARN("self"), IP: "10.0.0.9"}
)

func TestParseTarget(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		want    targetInfo
		wantErr string
	}{
		{
			name:   "defaults",
			target: testTarget,
			want:   targetInfo{cluster: testCluster, service: testService, port: testPort, refreshInterval: defaultRefreshInterval, requireHealthy: true},
		},
		{
			name:   "query parameters",
			target: testTarget + "?refreshInterval=500ms&requireHealthy=false",
			want:   targetInfo{cluster: testCluster, service: testService, port: testPort, refreshInterval: 500 * time.Millisecond, requireHealthy: false},
		},
		{name: "authority", target: "ecs://somewhere/" + testCluster + "/" + testService + ":" + testPort, wantErr: "authority"},
		{name: "missing port", target: "ecs:///" + testCluster + "/" + testService, wantErr: "missing port"},
		{name: "zero port", target: "ecs:///" + testCluster + "/" + testService + ":0", wantErr: "invalid port"},
		{name: "named port", target: "ecs:///" + testCluster + "/" + testService + ":grpc", wantErr: "invalid port"},
		{name: "missing cluster", target: "ecs:///" + testService + ":" + testPort, wantErr: "invalid target"},
		{name: "empty cluster", target: "ecs:////" + testService + ":" + testPort, wantErr: "invalid target"},
		{name: "extra path segment", target: "ecs:///" + testCluster + "/" + testService + "/extra:" + testPort, wantErr: "invalid target"},
		{name: "unparsable refresh interval", target: testTarget + "?refreshInterval=soon", wantErr: "invalid refreshInterval"},
		{name: "refresh interval too small", target: testTarget + "?refreshInterval=10ms", wantErr: "below the minimum"},
		{name: "unparsable require healthy", target: testTarget + "?requireHealthy=maybe", wantErr: "invalid requireHealthy"},
		{name: "unknown parameter", target: testTarget + "?refresh=2s", wantErr: "unknown query parameter"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseTarget(mustTarget(t, tc.target))
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// pollStep is the ECS and gRPC state to install before a single poll.
type pollStep struct {
	tasks       []types.Task
	listErr     error
	describeErr error
	updateErr   error
	noSelf      bool
}

func TestPoll(t *testing.T) {
	healthyA := runningTask("a", "10.0.0.1", types.HealthStatusHealthy)
	healthyB := runningTask("b", "10.0.0.2", types.HealthStatusHealthy)
	healthyC := runningTask("c", "10.0.0.3", types.HealthStatusHealthy)
	unknownB := runningTask("b", "10.0.0.2", types.HealthStatusUnknown)

	tests := []struct {
		name           string
		steps          []pollStep
		allowUnhealthy bool
		want           [][]string
		wantErrors     int
	}{
		{
			name: "publishes self and healthy tasks",
			steps: []pollStep{{tasks: []types.Task{
				healthyA,
				unknownB,
				runningTask("unhealthy", "10.0.0.3", types.HealthStatusUnhealthy),
				withLastStatus(runningTask("pending", "10.0.0.4", types.HealthStatusUnknown), "PENDING"),
				withDesiredStatus(runningTask("stopping", "10.0.0.5", types.HealthStatusHealthy), "STOPPED"),
			}}},
			want: [][]string{{"10.0.0.1:50053", "10.0.0.9:50053"}},
		},
		{
			name: "without require healthy",
			steps: []pollStep{{tasks: []types.Task{
				healthyA,
				unknownB,
				withLastStatus(runningTask("pending", "10.0.0.4", types.HealthStatusUnknown), "PENDING"),
			}}},
			allowUnhealthy: true,
			want:           [][]string{{"10.0.0.1:50053", "10.0.0.2:50053", "10.0.0.9:50053"}},
		},
		{
			name:  "publishes self when the first list fails",
			steps: []pollStep{{listErr: errors.New("access denied")}},
			want:  [][]string{{"10.0.0.9:50053"}},
		},
		{
			name:       "reports an error when there are no addresses",
			steps:      []pollStep{{noSelf: true}},
			wantErrors: 1,
		},
		{
			name: "adds tasks once healthy and removes stopped tasks",
			steps: []pollStep{
				{tasks: []types.Task{healthyA, unknownB}},
				{tasks: []types.Task{healthyA, healthyB}},
				{tasks: []types.Task{healthyB}},
			},
			want: [][]string{
				{"10.0.0.1:50053", "10.0.0.9:50053"},
				{"10.0.0.1:50053", "10.0.0.2:50053", "10.0.0.9:50053"},
				{"10.0.0.2:50053", "10.0.0.9:50053"},
			},
		},
		{
			name: "keeps the last known peers when a later list fails",
			steps: []pollStep{
				{tasks: []types.Task{healthyA}},
				{listErr: errors.New("throttled")},
			},
			want: [][]string{{"10.0.0.1:50053", "10.0.0.9:50053"}},
		},
		{
			// Removals are published even while describing new tasks fails.
			name: "publishes removals when describe fails",
			steps: []pollStep{
				{tasks: []types.Task{healthyA, healthyB}},
				{tasks: []types.Task{healthyB, healthyC}, describeErr: errors.New("throttled")},
				{tasks: []types.Task{healthyB, healthyC}},
			},
			want: [][]string{
				{"10.0.0.1:50053", "10.0.0.2:50053", "10.0.0.9:50053"},
				{"10.0.0.2:50053", "10.0.0.9:50053"},
				{"10.0.0.2:50053", "10.0.0.3:50053", "10.0.0.9:50053"},
			},
		},
		{
			name: "retries the self lookup",
			steps: []pollStep{
				{tasks: []types.Task{healthyA}, noSelf: true},
				{tasks: []types.Task{healthyA}},
			},
			want: [][]string{
				{"10.0.0.1:50053"},
				{"10.0.0.1:50053", "10.0.0.9:50053"},
			},
		},
		{
			name: "retries updates that gRPC rejected",
			steps: []pollStep{
				{tasks: []types.Task{healthyA}, updateErr: errors.New("no service config yet")},
				{tasks: []types.Task{healthyA}},
			},
			want: [][]string{{"10.0.0.1:50053", "10.0.0.9:50053"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var self *Self
			client := &fakeECS{}
			r, cc := newTestResolver(t, client, func(context.Context) (Self, error) {
				if self == nil {
					return Self{}, errors.New("no task metadata")
				}
				return *self, nil
			}, func(ti *targetInfo) { ti.requireHealthy = !tc.allowUnhealthy })

			for _, step := range tc.steps {
				self = &testSelf
				if step.noSelf {
					self = nil
				}
				client.setTasks(step.tasks...)
				client.setListErr(step.listErr)
				client.setDescribeErr(step.describeErr)
				cc.setUpdateErr(step.updateErr)

				r.poll(t.Context())
			}

			require.Equal(t, tc.want, cc.publishedStates())
			require.Len(t, cc.reportedErrors(), tc.wantErrors)
		})
	}
}

func TestPollOnlyDescribesTasksThatAreNotReady(t *testing.T) {
	client := &fakeECS{tasks: []types.Task{
		runningTask("a", "10.0.0.1", types.HealthStatusHealthy),
		runningTask("b", "10.0.0.2", types.HealthStatusUnknown),
	}}
	r, cc := newTestResolver(t, client, staticSelf(&testSelf))

	r.poll(t.Context())
	require.Equal(t, [][]string{{taskARN("a"), taskARN("b")}}, client.takeDescribeCalls())

	r.poll(t.Context())
	require.Equal(t, [][]string{{taskARN("b")}}, client.takeDescribeCalls())

	client.setTasks(
		runningTask("a", "10.0.0.1", types.HealthStatusHealthy),
		runningTask("b", "10.0.0.2", types.HealthStatusHealthy),
	)
	r.poll(t.Context())
	r.poll(t.Context())
	require.Equal(t, [][]string{{taskARN("b")}}, client.takeDescribeCalls())

	require.Len(t, cc.publishedStates(), 2)
}

func TestPollPaginatesAndBatchesDescribeTasks(t *testing.T) {
	tasks := make([]types.Task, 0, 150)
	want := make([]string, 0, 151)
	for i := range 150 {
		ip := "10.1.0." + strconv.Itoa(i+1)
		tasks = append(tasks, runningTask(strconv.Itoa(i), ip, types.HealthStatusHealthy))
		want = append(want, ip+":"+testPort)
	}
	want = append(want, testSelf.IP+":"+testPort)
	slices.Sort(want)

	client := &fakeECS{tasks: tasks, pageSize: 40}
	r, cc := newTestResolver(t, client, staticSelf(&testSelf))

	r.poll(t.Context())

	require.Equal(t, [][]string{want}, cc.publishedStates())
	calls := client.takeDescribeCalls()
	require.Len(t, calls, 2)
	require.Len(t, calls[0], describeTasksBatchSize)
	require.Len(t, calls[1], 50)
}

func TestTaskIPFallsBackToContainerNetworkInterfaces(t *testing.T) {
	task := types.Task{
		Containers: []types.Container{
			{},
			{NetworkInterfaces: []types.NetworkInterface{{PrivateIpv4Address: aws.String("10.0.0.7")}}},
		},
	}
	require.Equal(t, "10.0.0.7", taskIP(task))
	require.Empty(t, taskIP(types.Task{}))
}

func TestJitter(t *testing.T) {
	for range 100 {
		d := jitter(2 * time.Second)
		require.GreaterOrEqual(t, d, 1800*time.Millisecond)
		require.Less(t, d, 2200*time.Millisecond)
	}
	require.Equal(t, time.Duration(1), jitter(1))
}

func TestBuilder(t *testing.T) {
	defer goleak.VerifyNone(t, testutil.GoLeakIgnores()...)

	client := &fakeECS{tasks: []types.Task{runningTask("a", "10.0.0.1", types.HealthStatusHealthy)}}
	b := &builder{
		newClient: func(context.Context) (Client, error) { return client, nil },
		self:      staticSelf(&testSelf),
	}
	require.Equal(t, Scheme, b.Scheme())

	_, err := b.Build(mustTarget(t, "ecs:///"+testService+":"+testPort), newFakeClientConn(), resolver.BuildOptions{})
	require.ErrorContains(t, err, "invalid target")

	cc := newFakeClientConn()
	r, err := b.Build(mustTarget(t, testTarget+"?refreshInterval=1h"), cc, resolver.BuildOptions{})
	require.NoError(t, err)

	requirePublished(t, cc, [][]string{{"10.0.0.1:50053", "10.0.0.9:50053"}})

	client.setTasks(
		runningTask("a", "10.0.0.1", types.HealthStatusHealthy),
		runningTask("b", "10.0.0.2", types.HealthStatusHealthy),
	)
	r.ResolveNow(resolver.ResolveNowOptions{})
	r.ResolveNow(resolver.ResolveNowOptions{})

	requirePublished(t, cc, [][]string{
		{"10.0.0.1:50053", "10.0.0.9:50053"},
		{"10.0.0.1:50053", "10.0.0.2:50053", "10.0.0.9:50053"},
	})

	r.Close()
}

func newTestResolver(t *testing.T, client Client, selfFn func(context.Context) (Self, error), opts ...func(*targetInfo)) (*ecsResolver, *fakeClientConn) {
	t.Helper()

	ti := targetInfo{
		cluster:         testCluster,
		service:         testService,
		port:            testPort,
		refreshInterval: defaultRefreshInterval,
		requireHealthy:  true,
	}
	for _, opt := range opts {
		opt(&ti)
	}

	cc := newFakeClientConn()
	r := newResolver(ti, client, selfFn, cc)
	t.Cleanup(r.Close)
	return r, cc
}

func requirePublished(t *testing.T, cc *fakeClientConn, want [][]string) {
	t.Helper()

	var got [][]string
	require.Eventually(t, func() bool {
		got = cc.publishedStates()
		return slices.EqualFunc(want, got, slices.Equal)
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, want, got)
}

func staticSelf(self *Self) func(context.Context) (Self, error) {
	return func(context.Context) (Self, error) {
		if self == nil {
			return Self{}, errors.New("no task metadata")
		}
		return *self, nil
	}
}

func mustTarget(t *testing.T, raw string) resolver.Target {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	return resolver.Target{URL: *u}
}

func taskARN(id string) string {
	return "arn:aws:ecs:us-east-1:123456789012:task/" + testCluster + "/" + id
}

func runningTask(id, ip string, health types.HealthStatus) types.Task {
	return types.Task{
		TaskArn:       aws.String(taskARN(id)),
		DesiredStatus: aws.String(statusRunning),
		LastStatus:    aws.String(statusRunning),
		HealthStatus:  health,
		Attachments: []types.Attachment{{
			Type: aws.String(eniAttachmentType),
			Details: []types.KeyValuePair{
				{Name: aws.String("subnetId"), Value: aws.String("subnet-0123456789abcdef0")},
				{Name: aws.String(eniPrivateIPv4Field), Value: aws.String(ip)},
			},
		}},
	}
}

func withLastStatus(task types.Task, status string) types.Task {
	task.LastStatus = aws.String(status)
	return task
}

func withDesiredStatus(task types.Task, status string) types.Task {
	task.DesiredStatus = aws.String(status)
	return task
}

type fakeECS struct {
	pageSize int

	mu          sync.Mutex
	tasks       []types.Task // GUARDED_BY(mu)
	listErr     error        // GUARDED_BY(mu)
	describeErr error        // GUARDED_BY(mu)
	described   [][]string   // GUARDED_BY(mu)
}

func (f *fakeECS) ListTasks(_ context.Context, in *ecs.ListTasksInput, _ ...func(*ecs.Options)) (*ecs.ListTasksOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if aws.ToString(in.Cluster) != testCluster || aws.ToString(in.ServiceName) != testService {
		return nil, fmt.Errorf("listed cluster %q service %q", aws.ToString(in.Cluster), aws.ToString(in.ServiceName))
	}
	if f.listErr != nil {
		return nil, f.listErr
	}

	var arns []string
	for _, task := range f.tasks {
		if aws.ToString(task.DesiredStatus) == string(in.DesiredStatus) {
			arns = append(arns, aws.ToString(task.TaskArn))
		}
	}

	pageSize := f.pageSize
	if pageSize == 0 {
		pageSize = 100
	}
	start := 0
	if in.NextToken != nil {
		var err error
		if start, err = strconv.Atoi(*in.NextToken); err != nil {
			return nil, err
		}
	}
	end := min(start+pageSize, len(arns))

	out := &ecs.ListTasksOutput{TaskArns: arns[start:end]}
	if end < len(arns) {
		out.NextToken = aws.String(strconv.Itoa(end))
	}
	return out, nil
}

func (f *fakeECS) DescribeTasks(_ context.Context, in *ecs.DescribeTasksInput, _ ...func(*ecs.Options)) (*ecs.DescribeTasksOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if aws.ToString(in.Cluster) != testCluster {
		return nil, fmt.Errorf("described cluster %q", aws.ToString(in.Cluster))
	}

	f.described = append(f.described, slices.Clone(in.Tasks))
	if f.describeErr != nil {
		return nil, f.describeErr
	}

	out := &ecs.DescribeTasksOutput{}
	for _, task := range f.tasks {
		if slices.Contains(in.Tasks, aws.ToString(task.TaskArn)) {
			out.Tasks = append(out.Tasks, task)
		}
	}
	return out, nil
}

func (f *fakeECS) setTasks(tasks ...types.Task) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tasks = tasks
}

func (f *fakeECS) setListErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listErr = err
}

func (f *fakeECS) setDescribeErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.describeErr = err
}

// takeDescribeCalls returns the task batches described since the last call.
func (f *fakeECS) takeDescribeCalls() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	described := f.described
	f.described = nil
	return described
}

type fakeClientConn struct {
	resolver.ClientConn

	mu        sync.Mutex
	states    [][]string // GUARDED_BY(mu)
	errs      []error    // GUARDED_BY(mu)
	updateErr error      // GUARDED_BY(mu)
}

func newFakeClientConn() *fakeClientConn {
	return &fakeClientConn{}
}

func (cc *fakeClientConn) UpdateState(state resolver.State) error {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	if cc.updateErr != nil {
		return cc.updateErr
	}

	addrs := make([]string, 0, len(state.Addresses))
	for _, addr := range state.Addresses {
		addrs = append(addrs, addr.Addr)
	}
	cc.states = append(cc.states, addrs)
	return nil
}

func (cc *fakeClientConn) ReportError(err error) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.errs = append(cc.errs, err)
}

func (cc *fakeClientConn) setUpdateErr(err error) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.updateErr = err
}

func (cc *fakeClientConn) publishedStates() [][]string {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return slices.Clone(cc.states)
}

func (cc *fakeClientConn) reportedErrors() []error {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return slices.Clone(cc.errs)
}
