package kuberesolver

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	discoveryinformers "k8s.io/client-go/informers/discovery/v1"
	"k8s.io/client-go/kubernetes"
	discoverylisters "k8s.io/client-go/listers/discovery/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	kubernetesSchema        = "kubernetes"
	defaultNamespace        = "default"
	kubernetesNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	defaultResyncPeriod     = 30 * time.Minute
)

var (
	endpointsForTarget = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kuberesolver_endpoints",
			Help: "The total number of endpoints for a given target",
		},
		[]string{"target"},
	)
	addressesForTarget = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kuberesolver_addresses",
			Help: "The total number of addresses for a given target",
		},
		[]string{"target"},
	)
	clientLastUpdate = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kuberesolver_client_last_update",
			Help: "The last time the resolver client was updated",
		},
		[]string{"target"},
	)
)

type targetInfo struct {
	scheme            string
	serviceName       string
	serviceNamespace  string
	port              string
	resolveByPortName bool
	useFirstPort      bool
}

func (ti targetInfo) String() string {
	if ti.scheme != "" {
		return fmt.Sprintf("%s://%s/%s:%s", ti.scheme, ti.serviceNamespace, ti.serviceName, ti.port)
	} else {
		return fmt.Sprintf("kubernetes://%s/%s:%s", ti.serviceNamespace, ti.serviceName, ti.port)
	}
}

// RegisterInCluster registers the kuberesolver builder to grpc with kubernetes schema
func RegisterInCluster() {
	RegisterInClusterWithSchema(kubernetesSchema)
}

// RegisterInClusterWithSchema registers the kuberesolver builder to the grpc with custom schema
func RegisterInClusterWithSchema(schema string) {
	resolver.Register(NewBuilder(nil, schema))
}

// NewBuilder creates a kubeBuilder using client-go with informers
func NewBuilder(client kubernetes.Interface, schema string) resolver.Builder {
	return &kubeBuilder{
		k8sClient: client,
		schema:    schema,
	}
}

type kubeBuilder struct {
	k8sClient kubernetes.Interface
	schema    string
}

func splitServicePortNamespace(hpn string) (service, port, namespace string) {
	service = hpn

	colon := strings.LastIndexByte(service, ':')
	if colon != -1 {
		service, port = service[:colon], service[colon+1:]
	}

	// we want to split into the service name, namespace, and whatever else is left
	// this will support fully qualified service names, e.g. {service-name}.<namespace>.svc.<cluster-domain-name>.
	// Note that since we lookup the endpoints by service name and namespace, we don't care about the
	// cluster-domain-name, only that we can parse out the service name and namespace properly.
	parts := strings.SplitN(service, ".", 3)
	if len(parts) >= 2 {
		service, namespace = parts[0], parts[1]
	}

	return
}

func parseResolverTarget(target resolver.Target) (targetInfo, error) {
	var service, port, namespace string
	if target.URL.Host == "" {
		// kubernetes:///service.namespace:port
		service, port, namespace = splitServicePortNamespace(target.Endpoint())
	} else if target.URL.Port() == "" && target.Endpoint() != "" {
		// kubernetes://namespace/service:port
		service, port, _ = splitServicePortNamespace(target.Endpoint())
		namespace = target.URL.Hostname()
	} else {
		// kubernetes://service.namespace:port
		service, port, namespace = splitServicePortNamespace(target.URL.Host)
	}

	if service == "" {
		return targetInfo{}, fmt.Errorf("target %s must specify a service", &target.URL)
	}

	resolveByPortName := false
	useFirstPort := false
	if port == "" {
		useFirstPort = true
	} else if _, err := strconv.Atoi(port); err != nil {
		resolveByPortName = true
	}

	return targetInfo{
		scheme:            target.URL.Scheme,
		serviceName:       service,
		serviceNamespace:  namespace,
		port:              port,
		resolveByPortName: resolveByPortName,
		useFirstPort:      useFirstPort,
	}, nil
}

func getCurrentNamespaceOrDefault() string {
	ns, err := os.ReadFile(kubernetesNamespaceFile)
	if err != nil {
		return defaultNamespace
	}
	return string(ns)
}

func (b *kubeBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	if b.k8sClient == nil {
		// Try in-cluster config first
		config, err := rest.InClusterConfig()
		if err != nil {
			// Fall back to kubeconfig
			config, err = clientcmd.BuildConfigFromFlags("", "")
			if err != nil {
				return nil, fmt.Errorf("failed to create kubernetes config: %w", err)
			}
		}

		client, err := kubernetes.NewForConfig(config)
		if err != nil {
			return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
		}
		b.k8sClient = client
	}

	ti, err := parseResolverTarget(target)
	if err != nil {
		return nil, err
	}

	if ti.serviceNamespace == "" {
		ti.serviceNamespace = getCurrentNamespaceOrDefault()
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Create informer factory for the specific namespace
	informerFactory := informers.NewSharedInformerFactoryWithOptions(
		b.k8sClient,
		defaultResyncPeriod,
		informers.WithNamespace(ti.serviceNamespace),
	)

	endpointSliceInformer := informerFactory.Discovery().V1().EndpointSlices()

	r := &kResolver{
		target:                ti,
		ctx:                   ctx,
		cancel:                cancel,
		cc:                    cc,
		endpointSliceInformer: endpointSliceInformer,
		endpointSliceLister:   endpointSliceInformer.Lister(),
		informerFactory:       informerFactory,

		endpoints:      endpointsForTarget.WithLabelValues(ti.String()),
		addresses:      addressesForTarget.WithLabelValues(ti.String()),
		lastUpdateUnix: clientLastUpdate.WithLabelValues(ti.String()),
	}

	// Add event handler for EndpointSlice changes
	_, err = endpointSliceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if endpointSlice, ok := obj.(*discoveryv1.EndpointSlice); ok {
				if r.isRelevantEndpointSlice(endpointSlice) {
					r.resolve()
				}
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if endpointSlice, ok := newObj.(*discoveryv1.EndpointSlice); ok {
				if r.isRelevantEndpointSlice(endpointSlice) {
					r.resolve()
				}
			}
		},
		DeleteFunc: func(obj interface{}) {
			if endpointSlice, ok := obj.(*discoveryv1.EndpointSlice); ok {
				if r.isRelevantEndpointSlice(endpointSlice) {
					r.resolve()
				}
			}
		},
	})
	if err != nil {
		return nil, err
	}

	// Start informer
	r.informerFactory.Start(ctx.Done())

	// Wait for cache sync
	if !cache.WaitForCacheSync(ctx.Done(), endpointSliceInformer.Informer().HasSynced) {
		cancel()
		return nil, fmt.Errorf("failed to sync EndpointSlice cache")
	}

	// Initial resolution
	r.resolve()

	return r, nil
}

func (b *kubeBuilder) Scheme() string {
	return b.schema
}

type kResolver struct {
	target                targetInfo
	ctx                   context.Context
	cancel                context.CancelFunc
	cc                    resolver.ClientConn
	endpointSliceInformer discoveryinformers.EndpointSliceInformer
	endpointSliceLister   discoverylisters.EndpointSliceLister
	informerFactory       informers.SharedInformerFactory

	mu             sync.RWMutex
	endpoints      prometheus.Gauge
	addresses      prometheus.Gauge
	lastUpdateUnix prometheus.Gauge
}

func (k *kResolver) ResolveNow(resolver.ResolveNowOptions) {
	k.resolve()
}

func (k *kResolver) Close() {
	k.cancel()
}

func (k *kResolver) isRelevantEndpointSlice(endpointSlice *discoveryv1.EndpointSlice) bool {
	if endpointSlice.Namespace != k.target.serviceNamespace {
		return false
	}

	serviceName, exists := endpointSlice.Labels["kubernetes.io/service-name"]
	return exists && serviceName == k.target.serviceName
}

func (k *kResolver) makeAddresses(endpointSlices []*discoveryv1.EndpointSlice) []resolver.Address {
	var allAddresses []resolver.Address

	// Aggregate addresses from all EndpointSlices
	for _, slice := range endpointSlices {
		port := k.target.port

		// Find the correct port
		for _, p := range slice.Ports {
			if k.target.useFirstPort {
				if p.Port != nil {
					port = strconv.Itoa(int(*p.Port))
					break
				}
			} else if k.target.resolveByPortName && p.Name != nil && *p.Name == k.target.port {
				if p.Port != nil {
					port = strconv.Itoa(int(*p.Port))
					break
				}
			}
		}

		// If no port found and we have ports, use the first one
		if len(port) == 0 && len(slice.Ports) > 0 && slice.Ports[0].Port != nil {
			port = strconv.Itoa(int(*slice.Ports[0].Port))
		}

		// Extract addresses from all ready endpoints
		for _, endpoint := range slice.Endpoints {
			if endpoint.Conditions.Ready != nil && *endpoint.Conditions.Ready {
				for _, address := range endpoint.Addresses {
					allAddresses = append(allAddresses, resolver.Address{
						Addr:       net.JoinHostPort(address, port),
						ServerName: fmt.Sprintf("%s.%s", k.target.serviceName, k.target.serviceNamespace),
						Metadata:   nil,
					})
				}
			}
		}
	}

	return allAddresses
}

func (k *kResolver) resolve() {
	k.mu.Lock()
	defer k.mu.Unlock()

	// Query the informer cache instead of making API calls
	selector := labels.SelectorFromSet(labels.Set{
		"kubernetes.io/service-name": k.target.serviceName,
	})

	endpointSlices, err := k.endpointSliceLister.EndpointSlices(k.target.serviceNamespace).List(selector)
	if err != nil {
		grpclog.Errorf("kuberesolver: failed to list endpointslices from cache: %v", err)
		return
	}

	addrs := k.makeAddresses(endpointSlices)

	if len(addrs) > 0 {
		err := k.cc.UpdateState(resolver.State{
			Addresses: addrs,
		})
		if err != nil {
			grpclog.Errorf("kuberesolver: failed to update state: %v", err)
		}
		k.lastUpdateUnix.Set(float64(time.Now().Unix()))
	}

	// Update metrics
	totalEndpoints := 0
	for _, slice := range endpointSlices {
		totalEndpoints += len(slice.Endpoints)
	}

	k.endpoints.Set(float64(totalEndpoints))
	k.addresses.Set(float64(len(addrs)))

	grpclog.Infof("kuberesolver: resolved %d addresses for service %s.%s", len(addrs), k.target.serviceName, k.target.serviceNamespace)
}
