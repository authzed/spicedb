// Package ecs implements a gRPC name resolver that discovers the tasks of an
// Amazon ECS service by polling the ECS API.
//
// SpiceDB's consistent hashring balancer only learns about cluster membership
// from its resolver. The DNS resolver re-resolves only after a connection
// fails, and at most once every 30 seconds, which leaves stale peers on the
// ring after tasks are replaced. This resolver polls ECS every few seconds
// instead.
//
// Target format:
//
//	ecs:///<cluster>/<service>:<port>[?refreshInterval=2s&requireHealthy=true]
//
// Peers are the service's tasks whose desired and last status are RUNNING
// and, unless requireHealthy=false, whose container health checks pass. The
// task running the resolver is always included, so the address list is never
// empty while the task itself is up.
package ecs

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"google.golang.org/grpc/resolver"

	log "github.com/authzed/spicedb/internal/logging"
)

// Scheme is the gRPC target scheme handled by this resolver.
const Scheme = "ecs"

const (
	defaultRefreshInterval = 2 * time.Second
	minRefreshInterval     = 100 * time.Millisecond
	minResolveNowInterval  = time.Second
	pollTimeout            = 10 * time.Second

	// DescribeTasks accepts at most 100 tasks per call.
	describeTasksBatchSize = 100

	statusRunning       = "RUNNING"
	eniAttachmentType   = "ElasticNetworkInterface"
	eniPrivateIPv4Field = "privateIPv4Address"
)

// Client is the subset of the ECS API used by the resolver.
type Client interface {
	ecs.ListTasksAPIClient
	DescribeTasks(ctx context.Context, params *ecs.DescribeTasksInput, optFns ...func(*ecs.Options)) (*ecs.DescribeTasksOutput, error)
}

// Self identifies the ECS task the resolver runs in.
type Self struct {
	TaskARN string
	IP      string
}

// Register registers the ECS resolver with gRPC under the "ecs" scheme.
func Register() {
	resolver.Register(NewBuilder())
}

// NewBuilder returns a resolver.Builder that uses the default AWS
// configuration chain and the ECS task metadata endpoint.
func NewBuilder() resolver.Builder {
	return &builder{newClient: defaultClient, self: taskMetadataSelf}
}

type builder struct {
	newClient func(ctx context.Context) (Client, error)
	self      func(ctx context.Context) (Self, error)
}

func defaultClient(ctx context.Context) (Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}
	return ecs.NewFromConfig(cfg), nil
}

func (b *builder) Scheme() string { return Scheme }

func (b *builder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	t, err := parseTarget(target)
	if err != nil {
		return nil, err
	}

	client, err := b.newClient(context.Background())
	if err != nil {
		return nil, fmt.Errorf("ecs resolver: %w", err)
	}

	r := newResolver(t, client, b.self, cc)
	r.wg.Add(1)
	go r.run()
	return r, nil
}

type targetInfo struct {
	cluster         string
	service         string
	port            string
	refreshInterval time.Duration
	requireHealthy  bool
}

func (t targetInfo) String() string {
	return t.cluster + "/" + t.service + ":" + t.port
}

func parseTarget(target resolver.Target) (targetInfo, error) {
	const usage = "expected ecs:///<cluster>/<service>:<port>"

	if target.URL.Host != "" {
		return targetInfo{}, fmt.Errorf("ecs resolver: authority %q is not supported, %s", target.URL.Host, usage)
	}

	hostPart, port, err := net.SplitHostPort(target.Endpoint())
	if err != nil {
		return targetInfo{}, fmt.Errorf("ecs resolver: invalid target %q, %s: %w", target.Endpoint(), usage, err)
	}
	if p, err := strconv.ParseUint(port, 10, 16); err != nil || p == 0 {
		return targetInfo{}, fmt.Errorf("ecs resolver: invalid port %q, %s", port, usage)
	}

	cluster, service, ok := strings.Cut(hostPart, "/")
	if !ok || cluster == "" || service == "" || strings.Contains(service, "/") {
		return targetInfo{}, fmt.Errorf("ecs resolver: invalid target %q, %s", target.Endpoint(), usage)
	}

	t := targetInfo{
		cluster:         cluster,
		service:         service,
		port:            port,
		refreshInterval: defaultRefreshInterval,
		requireHealthy:  true,
	}

	for key, values := range target.URL.Query() {
		value := values[len(values)-1]
		switch key {
		case "refreshInterval":
			d, err := time.ParseDuration(value)
			if err != nil {
				return targetInfo{}, fmt.Errorf("ecs resolver: invalid refreshInterval %q: %w", value, err)
			}
			if d < minRefreshInterval {
				return targetInfo{}, fmt.Errorf("ecs resolver: refreshInterval %s is below the minimum of %s", d, minRefreshInterval)
			}
			t.refreshInterval = d
		case "requireHealthy":
			b, err := strconv.ParseBool(value)
			if err != nil {
				return targetInfo{}, fmt.Errorf("ecs resolver: invalid requireHealthy %q: %w", value, err)
			}
			t.requireHealthy = b
		default:
			return targetInfo{}, fmt.Errorf("ecs resolver: unknown query parameter %q", key)
		}
	}

	return t, nil
}

type ecsResolver struct {
	target targetInfo
	client Client
	selfFn func(ctx context.Context) (Self, error)
	cc     resolver.ClientConn

	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	resolveNow chan struct{}

	// Only accessed by the polling goroutine.
	self      *Self
	ready     map[string]string // task ARN -> host:port
	published []string
	lastPoll  time.Time
}

func newResolver(t targetInfo, client Client, selfFn func(ctx context.Context) (Self, error), cc resolver.ClientConn) *ecsResolver {
	ctx, cancel := context.WithCancel(context.Background())
	return &ecsResolver{
		target:     t,
		client:     client,
		selfFn:     selfFn,
		cc:         cc,
		ctx:        ctx,
		cancel:     cancel,
		resolveNow: make(chan struct{}, 1),
		ready:      make(map[string]string),
	}
}

func (r *ecsResolver) ResolveNow(resolver.ResolveNowOptions) {
	select {
	case r.resolveNow <- struct{}{}:
	default:
	}
}

func (r *ecsResolver) Close() {
	r.cancel()
	r.wg.Wait()
}

func (r *ecsResolver) run() {
	defer r.wg.Done()

	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.resolveNow:
			timer.Reset(max(minResolveNowInterval-time.Since(r.lastPoll), 0))
			continue
		case <-timer.C:
		}

		r.lastPoll = time.Now()
		r.poll(r.ctx)
		timer.Reset(jitter(r.target.refreshInterval))
	}
}

func (r *ecsResolver) poll(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, pollTimeout)
	defer cancel()

	if r.self == nil {
		self, err := r.selfFn(ctx)
		if err != nil {
			log.Warn().Err(err).Str("target", r.target.String()).Msg("ecs resolver: failed to read task metadata, this task is not in its own peer list yet")
		} else {
			r.self = &self
			log.Info().Str("target", r.target.String()).Str("task", self.TaskARN).Str("ip", self.IP).Msg("ecs resolver: discovered own task")
		}
	}

	arns, err := r.listTasks(ctx)
	if err != nil {
		if parent.Err() != nil {
			return
		}
		log.Warn().Err(err).Str("target", r.target.String()).Msg("ecs resolver: failed to list tasks, keeping the last known peers")
		if r.published == nil {
			r.publish()
		}
		return
	}

	r.prune(arns)
	describeErr := r.describePending(ctx, arns)
	if describeErr != nil && parent.Err() != nil {
		return
	}

	// Removals are published even if describing new tasks failed, so that
	// draining tasks leave the ring as early as possible.
	r.publish()

	if describeErr != nil {
		log.Warn().Err(describeErr).Str("target", r.target.String()).Msg("ecs resolver: failed to describe new tasks, they will be retried")
	}
}

func (r *ecsResolver) listTasks(ctx context.Context) ([]string, error) {
	paginator := ecs.NewListTasksPaginator(r.client, &ecs.ListTasksInput{
		Cluster:       aws.String(r.target.cluster),
		ServiceName:   aws.String(r.target.service),
		DesiredStatus: types.DesiredStatusRunning,
	})

	var arns []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing tasks: %w", err)
		}
		arns = append(arns, page.TaskArns...)
	}
	return arns, nil
}

func (r *ecsResolver) prune(arns []string) {
	for arn := range r.ready {
		if !slices.Contains(arns, arn) {
			delete(r.ready, arn)
		}
	}
}

// describePending describes listed tasks that are not ready yet. Tasks that
// were ready stay ready until they stop being listed, which happens as soon
// as ECS sets their desired status to STOPPED.
func (r *ecsResolver) describePending(ctx context.Context, arns []string) error {
	pending := make([]string, 0, len(arns))
	for _, arn := range arns {
		if _, ok := r.ready[arn]; !ok {
			pending = append(pending, arn)
		}
	}

	var errs []error
	for batch := range slices.Chunk(pending, describeTasksBatchSize) {
		out, err := r.client.DescribeTasks(ctx, &ecs.DescribeTasksInput{
			Cluster: aws.String(r.target.cluster),
			Tasks:   batch,
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("describing tasks: %w", err))
			continue
		}

		for _, task := range out.Tasks {
			if !r.isReady(task) {
				continue
			}
			if ip := taskIP(task); ip != "" {
				r.ready[aws.ToString(task.TaskArn)] = net.JoinHostPort(ip, r.target.port)
			}
		}
	}
	return errors.Join(errs...)
}

func (r *ecsResolver) isReady(task types.Task) bool {
	if aws.ToString(task.DesiredStatus) != statusRunning || aws.ToString(task.LastStatus) != statusRunning {
		return false
	}
	return !r.target.requireHealthy || task.HealthStatus == types.HealthStatusHealthy
}

func (r *ecsResolver) publish() {
	addrs := make([]string, 0, len(r.ready)+1)
	for _, addr := range r.ready {
		addrs = append(addrs, addr)
	}
	if r.self != nil {
		addrs = append(addrs, net.JoinHostPort(r.self.IP, r.target.port))
	}
	slices.Sort(addrs)
	addrs = slices.Compact(addrs)

	if len(addrs) == 0 {
		r.cc.ReportError(fmt.Errorf("ecs resolver: no ready tasks for service %q in cluster %q", r.target.service, r.target.cluster))
		return
	}
	if slices.Equal(addrs, r.published) {
		return
	}

	state := resolver.State{Addresses: make([]resolver.Address, 0, len(addrs))}
	for _, addr := range addrs {
		state.Addresses = append(state.Addresses, resolver.Address{Addr: addr})
	}
	if err := r.cc.UpdateState(state); err != nil {
		log.Warn().Err(err).Str("target", r.target.String()).Strs("peers", addrs).Msg("ecs resolver: gRPC rejected the peer list, it will be retried")
		return
	}

	r.published = addrs
	log.Info().Str("target", r.target.String()).Strs("peers", addrs).Msg("ecs resolver: updated peers")
}

func taskIP(task types.Task) string {
	for _, attachment := range task.Attachments {
		if aws.ToString(attachment.Type) != eniAttachmentType {
			continue
		}
		for _, detail := range attachment.Details {
			if aws.ToString(detail.Name) == eniPrivateIPv4Field {
				return aws.ToString(detail.Value)
			}
		}
	}
	for _, container := range task.Containers {
		for _, ni := range container.NetworkInterfaces {
			if ip := aws.ToString(ni.PrivateIpv4Address); ip != "" {
				return ip
			}
		}
	}
	return ""
}

// jitter spreads polls by ±10% so tasks started together do not call the ECS
// API in lockstep.
func jitter(d time.Duration) time.Duration {
	spread := int64(d) / 5
	if spread <= 0 {
		return d
	}
	return d - time.Duration(spread/2) + time.Duration(rand.Int64N(spread)) //nolint:gosec // jitter does not need a cryptographic source
}
