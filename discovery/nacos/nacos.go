package nacos

import (
	"context"
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/peggypig/nacos-go/clients"
	"github.com/peggypig/nacos-go/clients/service_client"
	"github.com/peggypig/nacos-go/common/constant"
	"github.com/peggypig/nacos-go/vo"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/util/strutil"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	refreshInterval = 15 * time.Second
	getTimeout      = 30 * time.Second
	beatInterval    = 5 * time.Second
	retryInterval   = 15 * time.Second

	// metaDataLabel is the prefix for the labels mapping to a target's metadata.
	metaDataLabel = model.MetaLabelPrefix + "nacos_metadata_"
	// serviceMetaDataLabel is the prefix for the labels mapping to a target's service metadata.
	serviceMetaDataLabel = model.MetaLabelPrefix + "nacos_service_metadata_"
	// serviceLabel is the name of the label containing the service name.
	serviceLabel = model.MetaLabelPrefix + "nacos_service"
	// serviceIDLabel is the name of the label containing the service ID.
	serviceIDLabel = model.MetaLabelPrefix + "nacos_service_id"
	// serviceNameLabel is the name of the label containing the service Name.
	serviceNameLabel = model.MetaLabelPrefix + "nacos_service_name"
	// clusterNameLabel is the name of the label containing the service Cluster Name.
	clusterNameLabel = model.MetaLabelPrefix + "nacos_service_cluster"
	// serviceValidLabel is the name of the label containing the service Valid.
	serviceValidLabel = model.MetaLabelPrefix + "nacos_service_valid"

	// Constants for instrumentation.
	namespace = "prometheus"
)

var (
	rpcFailuresCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "sd_nacos_rpc_failures_total",
			Help:      "The number of Nacos RPC call failures.",
		})
	rpcDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: namespace,
			Name:      "sd_nacos_rpc_duration_seconds",
			Help:      "The duration of a Nacos RPC call in seconds.",
		},
		[]string{"endpoint", "call"},
	)

	// DefaultSDConfig is the default Nacos SD configuration.
	DefaultSDConfig = SDConfig{
		ContextPath:     "/nacos",
		Servers:         []string{"localhost:8848"},
		RefreshInterval: model.Duration(refreshInterval),
		GetTimeout:      model.Duration(getTimeout),
	}
)

// SDConfig is the configuration for Nacos service discovery.
type SDConfig struct {
	Servers     []string `yaml:"servers,omitempty"`
	ContextPath string   `yaml:"contextPath,omitempty"`

	RefreshInterval model.Duration `yaml:"refresh_interval,omitempty"`

	GetTimeout model.Duration `yaml:"timeout,omitempty"`

	// The list of services for which targets are discovered.
	// Defaults to all services if empty.
	Services []string `yaml:"services,omitempty"`
	// An optional meta used to filter instances inside a service.
	ServiceMeta string `yaml:"service_meta,omitempty"`
	// Desired node metadata.
	NodeMeta string `yaml:"node_meta,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *SDConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultSDConfig
	type plain SDConfig
	err := unmarshal((*plain)(c))
	if err != nil {
		return err
	}
	if len(c.Servers) == 0 {
		return fmt.Errorf("nacos SD configuration requires a server address")
	}
	return nil
}

func init() {
	prometheus.MustRegister(rpcFailuresCount)
	prometheus.MustRegister(rpcDuration)

	// Initialize metric vectors.
	rpcDuration.WithLabelValues("catalog", "service")
	rpcDuration.WithLabelValues("catalog", "services")
}

// Discovery retrieves target information from a Nacos server
// and updates them via watches.
type Discovery struct {
	client             service_client.IServiceClient
	watchedServices    []string // Set of services which will be discovered.
	watchedServiceMeta string   // A tag used to filter instances of a service.
	watchedNodeMeta    string
	refreshInterval    time.Duration
	finalizer          func()
	logger             log.Logger
}

// NewDiscovery returns a new Discovery for the given config.
func NewDiscovery(conf *SDConfig, logger log.Logger) (discovery *Discovery, err error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	clientConfig := constant.ClientConfig{
		TimeoutMs:      uint64(conf.GetTimeout),
		ListenInterval: uint64(conf.RefreshInterval),
		BeatInterval:   uint64(beatInterval),
	}
	serverConfigs := []constant.ServerConfig{}
	for _, server := range conf.Servers {
		items := strings.Split(server, ":")
		if len(items) != 2 {
			level.Error(logger).Log("server url is error", server)
			continue
		}
		ip := items[0]
		port, err := strconv.Atoi(items[1])
		if err != nil || len(ip) < 4 {
			level.Error(logger).Log("server url is error", server)
			continue
		}
		serverConfigs = append(serverConfigs, constant.ServerConfig{IpAddr: ip, Port: uint64(port), ContextPath: conf.ContextPath})
	}

	if len(serverConfigs) == 0 {
		level.Error(logger).Log("msg", "no valid nacos server exists")
		err = errors.New("no valid nacos server exists")
		return nil, err
	}

	// 如果参数设置不合法，将抛出error
	client, err := clients.CreateServiceClient(map[string]interface{}{
		"serverConfigs": serverConfigs,
		"clientConfig":  clientConfig,
	})
	if err != nil {
		level.Error(logger).Log("create nacos client error", err)
		return
	}

	discovery = &Discovery{
		client:             client,
		logger:             logger,
		refreshInterval:    time.Duration(conf.RefreshInterval),
		watchedServiceMeta: conf.ServiceMeta,
		watchedNodeMeta:    conf.NodeMeta,
		watchedServices:    conf.Services,
	}
	return discovery, nil
}

// Initialize the Discoverer run.
func (d *Discovery) initialize(ctx context.Context) {
	// Loop until we manage to get the local datacenter.
	for {
		// We have to check the context at least once. The checks during channel sends
		// do not guarantee that.
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Do nothing! We are good to go.
		return
	}
}

// Run implements the Discoverer interface.
func (d *Discovery) Run(ctx context.Context, ch chan<- []*targetgroup.Group) {
	if d.finalizer != nil {
		defer d.finalizer()
	}
	d.initialize(ctx)

	if len(d.watchedServices) == 0 || d.watchedServiceMeta != "" {
		// We need to watch the catalog.
		ticker := time.NewTicker(d.refreshInterval)

		// Watched services and their cancellation functions.
		services := make(map[string]func())
		var lastIndex uint64

		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			default:
				d.watchServices(ctx, ch, &lastIndex, services)
				<-ticker.C
			}
		}

	} else {
		// We only have fully defined services.
		for _, name := range d.watchedServices {
			d.watchService(ctx, ch, name)
		}
		<-ctx.Done()
	}
}

// Watch the catalog for new services we would like to watch. This is called only
// when we don't know yet the names of the services and need to ask nacos the
// entire list of services.
func (d *Discovery) watchServices(ctx context.Context, ch chan<- []*targetgroup.Group, lastIndex *uint64, services map[string]func()) error {
	level.Debug(d.logger).Log("msg", "Watching services", "tag", d.watchedServiceMeta)

	t0 := time.Now()

	// get all services one time
	srvs, err := d.client.GetServiceList(vo.GetServiceListParam{StartPage: 1, PageSize: 10000000})
	elapsed := time.Since(t0)
	rpcDuration.WithLabelValues("catalog", "services").Observe(elapsed.Seconds())

	if err != nil {
		level.Error(d.logger).Log("msg", "Error refreshing service list", "err", err)
		rpcFailuresCount.Inc()
		time.Sleep(retryInterval)
		return err
	}

	// If the index equals the previous one, the watch timed out with no update.

	// Check for new services.
	current := make(map[string]interface{})
	for _, srv := range srvs.ServiceList {
		name := srv.Name
		current[name] = 1
		if _, ok := services[name]; ok {
			continue // We are already watching the service.
		}

		detail, err := d.client.GetServiceDetail(vo.GetServiceDetailParam{ServiceName: srv.Name})
		if err == nil {
			if !d.shouldWatch(name, detail.Service.Metadata) {
				continue
			}
		} else {
			level.Error(d.logger).Log("msg", "Error getting service detail info", "err", err)
			level.Error(d.logger).Log("msg", "filter condition will not be used", "service", name)
		}

		wctx, cancel := context.WithCancel(ctx)
		d.watchService(wctx, ch, name)
		services[name] = cancel
	}

	// Check for removed services.
	for name, cancel := range services {
		if _, ok := current[name]; !ok {
			// Call the watch cancellation function.
			cancel()
			delete(services, name)
			// Send clearing target group.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ch <- []*targetgroup.Group{{Source: name}}:
			}
		}
	}
	return nil
}

// shouldWatch returns whether the service of the given name should be watched.
func (d *Discovery) shouldWatch(name string, meta map[string]string) bool {
	return d.shouldWatchFromName(name) && d.shouldWatchFromMeta(d.watchedServiceMeta, meta)
}

// shouldWatch returns whether the service of the given name should be watched based on its name.
func (d *Discovery) shouldWatchFromName(name string) bool {
	// If there's no fixed set of watched services, we watch everything.
	if len(d.watchedServices) == 0 {
		return true
	}
	for _, sn := range d.watchedServices {
		if sn == name {
			return true
		}
	}
	return false
}

// shouldWatch returns whether the service of the given name should be watched based on its tags.
// This gets called when the user doesn't specify a list of services in order to avoid watching
// *all* services. Details in https://github.com/prometheus/prometheus/pull/3814
func (d *Discovery) shouldWatchFromMeta(userMeta string, meta map[string]string) bool {
	// If there's no fixed set of watched tags, we watch everything.
	if userMeta == "" {
		return true
	}
	metas := strings.Split(userMeta, "=")
	if len(metas) != 2 {
		level.Error(d.logger).Log("msg", "invalid format of watchedServiceMeta", "value", userMeta)
		return true
	}
	key := metas[0]
	value := metas[1]

	if value == meta[key] {
		return true
	}
	return false
}

// Start watching a service.
func (d *Discovery) watchService(ctx context.Context, ch chan<- []*targetgroup.Group, name string) {
	srv := &nacosService{
		discovery: d,
		client:    d.client,
		name:      name,
		tag:       d.watchedServiceMeta,
		labels: model.LabelSet{
			serviceLabel: model.LabelValue(name),
		},
		logger: d.logger,
	}

	go func() {
		ticker := time.NewTicker(d.refreshInterval)
		var lastChecksum string
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			default:
				srv.watch(ctx, ch, &lastChecksum)
				<-ticker.C
			}
		}
	}()
}

// nacosService contains data belonging to the same service.
type nacosService struct {
	name         string
	tag          string
	labels       model.LabelSet
	discovery    *Discovery
	client       service_client.IServiceClient
	tagSeparator string
	logger       log.Logger
}

// Get updates for a service.
func (srv *nacosService) watch(ctx context.Context, ch chan<- []*targetgroup.Group, lastChecksum *string) error {
	level.Debug(srv.logger).Log("msg", "Watching service", "service", srv.name, "tag", srv.tag)

	t0 := time.Now()
	service, err := srv.client.GetService(vo.GetServiceParam{ServiceName: srv.name})
	elapsed := time.Since(t0)
	rpcDuration.WithLabelValues("catalog", "service").Observe(elapsed.Seconds())

	// Check the context before in order to exit early.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Continue.
	}
	if err != nil {
		level.Error(srv.logger).Log("msg", "Error refreshing service", "service", srv.name, "tag", srv.tag, "err", err)
		rpcFailuresCount.Inc()
		time.Sleep(retryInterval)
		return err
	}
	refTime := strconv.FormatUint(service.LastRefTime, 10)
	check := strings.TrimSuffix(service.Checksum, refTime)
	// no update
	if check == *lastChecksum {
		return nil
	}

	tgroup := targetgroup.Group{
		Source:  srv.name,
		Labels:  srv.labels,
		Targets: []model.LabelSet{},
	}

	for _, node := range service.Hosts {
		if !srv.discovery.shouldWatchFromMeta(srv.discovery.watchedNodeMeta, node.Metadata) {
			continue
		}

		addr := net.JoinHostPort(node.Ip, fmt.Sprintf("%d", node.Port))
		labels := model.LabelSet{
			model.AddressLabel: model.LabelValue(addr),
			serviceIDLabel:     model.LabelValue(node.InstanceId),
			serviceNameLabel:   model.LabelValue(node.ServiceName),
			clusterNameLabel:   model.LabelValue(node.ClusterName),
			serviceValidLabel:  model.LabelValue(strconv.FormatBool(node.Valid)),
		}

		// Add all key/value pairs from the node's metadata as their own labels.
		for k, v := range node.Metadata {
			name := strutil.SanitizeLabelName(k)
			labels[metaDataLabel+model.LabelName(name)] = model.LabelValue(v)
		}
		// Add all key/value pairs from the service's metadata as their own labels.
		for k, v := range service.Metadata {
			name := strutil.SanitizeLabelName(k)
			labels[serviceMetaDataLabel+model.LabelName(name)] = model.LabelValue(v)
		}

		tgroup.Targets = append(tgroup.Targets, labels)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch <- []*targetgroup.Group{&tgroup}:
		{
			// save last checksum
			lastChecksum = &check
		}
	}
	return nil
}
