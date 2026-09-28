package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	admin "github.com/amigoer/rocketmq-admin-go"
)

// session is one open RocketMQ connection: a NameServer client, the local
// loopback routers that keep broker addresses reachable, and the resolved
// cluster identity. One sidecar process serves every connection, so sessions
// are keyed by `connection.id` and never share mutable state.
type session struct {
	operations     sync.RWMutex
	lifetime       context.Context
	cancelLifetime context.CancelFunc
	dashboard      *dashboardState
	mu             sync.RWMutex
	client         *admin.Client
	proxies        *proxyManager
	connection     connectionConfig
	clusterName    string
	brokerAddr     string
	name           string
	serverAddr     string
	aclEnabled     bool
	readOnly       bool
	pageSize       int
	closed         bool
}

// newSession validates a connection request and prepares (but does not open)
// the session. Call probe to actually connect.
func newSession(value lifecycleParams) (*session, error) {
	if strings.TrimSpace(value.Connection.ID) == "" {
		return nil, errors.New("connection id is required")
	}
	config, err := buildConnectionConfig(value)
	if err != nil {
		return nil, err
	}
	pageSize := value.Connection.Config.PageSize
	if pageSize < 1 || pageSize > 500 {
		pageSize = 50
	}
	lifetime, cancelLifetime := context.WithCancel(context.Background())
	return &session{
		lifetime: lifetime, cancelLifetime: cancelLifetime,
		dashboard:  newDashboardState(value.Connection.ID + "|" + strings.Join(config.NameServers, ";")),
		connection: config,
		name:       strings.TrimSpace(value.Connection.Name),
		serverAddr: strings.Join(config.NameServers, ";"),
		readOnly:   value.Connection.ReadOnly || value.Connection.Config.ReadOnly,
		pageSize:   pageSize,
	}, nil
}

// buildConnectionConfig turns the host payload into the agent's connection
// config, resolving which endpoint the request must actually dial.
func buildConnectionConfig(value lifecycleParams) (connectionConfig, error) {
	config := connectionConfig{
		UseTLS:         value.Connection.Config.UseTLS,
		VIPChannel:     value.Connection.Config.VIPChannel,
		ProxyAddr:      strings.TrimSpace(value.Connection.Config.ProxyAddr),
		ClusterName:    strings.TrimSpace(value.Connection.Config.ClusterName),
		AccessKey:      strings.TrimSpace(value.Connection.ConnectionSecrets["access_key"]),
		SecretKey:      value.Connection.ConnectionSecrets["secret_key"],
		RequestTimeout: defaultRequestTimeout,
		ConnectTimeout: defaultConnectTimeout,
		TLSSkipVerify:  value.Connection.Config.TLSSkipVerify,
	}

	nameservers := splitAddresses(value.Connection.Config.NamesrvAddr)
	if len(nameservers) == 0 {
		host := strings.TrimSpace(value.Connection.Host)
		if host == "" || value.Connection.Port < 1 || value.Connection.Port > 65535 {
			return connectionConfig{}, errors.New("a NameServer address is required: set namesrv_addr or a host and port")
		}
		nameservers = []string{net.JoinHostPort(host, strconv.Itoa(value.Connection.Port))}
	}
	for _, address := range nameservers {
		host, port := parseSocketAddress(address)
		if host == "" || port == "" {
			return connectionConfig{}, fmt.Errorf("invalid NameServer address %q: expected host:port", address)
		}
	}
	config.NameServers = nameservers
	if value.Runtime.Proxy != nil && !isSOCKS5Proxy(value.Runtime.Proxy) {
		return connectionConfig{}, errors.New("invalid SOCKS5 runtime proxy")
	}

	switch {
	case isSOCKS5Proxy(value.Runtime.Proxy):
		// Multi-endpoint route: DBX resolved one transport layer that can reach
		// every advertised address, so keep the original NameServer addresses
		// and let the proxy manager route each dial through it.
		config.SocksProxy = &socksProxyConfig{
			Host:     value.Runtime.Proxy.Host,
			Port:     value.Runtime.Proxy.Port,
			Username: value.Runtime.Proxy.Username,
			Password: value.Runtime.Proxy.Password,
		}
	case hasActiveTransportLayer(value.Connection.TransportLayers) && value.Runtime.Host != "" && value.Runtime.Port > 0:
		// Static forward: the tunnel reaches exactly one endpoint, which can
		// only be the NameServer. Broker operations will fail through it, which
		// is why the connection provider declares proxy_route.
		config.NameServers = []string{net.JoinHostPort(value.Runtime.Host, strconv.Itoa(value.Runtime.Port))}
	}
	return config, nil
}

// isSOCKS5Proxy reports whether the host handed us a usable SOCKS5 route.
func isSOCKS5Proxy(proxy *runtimeProxy) bool {
	if proxy == nil || proxy.Host == "" || proxy.Port < 1 || proxy.Port > 65535 {
		return false
	}
	proxyType := strings.ToLower(strings.TrimSpace(proxy.Type))
	return proxyType == "" || proxyType == "socks5"
}

func hasActiveTransportLayer(layers []transportLayer) bool {
	for _, layer := range layers {
		if layer.Enabled == nil || *layer.Enabled {
			return true
		}
	}
	return false
}

// probe opens the cluster connection and stores the resolved identity.
func (s *session) probe() error {
	client, proxies, clusterInfo, err := buildClient(s.connection)
	if err != nil {
		return err
	}
	clusterName := resolveClusterName(clusterInfo, s.connection.ClusterName)
	brokerAddr, err := resolveBrokerAddr(clusterInfo, s.connection, proxies)
	if err != nil {
		client.Close()
		proxies.Close()
		return err
	}
	// NameServer discovery alone does not authenticate against a Broker.
	ctx, cancel := context.WithTimeout(s.lifetime, s.connection.ConnectTimeout)
	_, err = client.FetchBrokerRuntimeStats(ctx, brokerAddr)
	cancel()
	if err != nil {
		client.Close()
		proxies.Close()
		return fmt.Errorf("Broker authentication/connectivity check failed: %w", err)
	}
	aclEnabled := probeACLEnabled(client, brokerAddr, s.connection)

	s.mu.Lock()
	s.client = client
	s.proxies = proxies
	s.clusterName = clusterName
	s.brokerAddr = brokerAddr
	s.aclEnabled = aclEnabled
	s.mu.Unlock()
	return nil
}

// probeACLEnabled reports whether the cluster enforces ACL authentication. The
// workbench hides the permission tab when it does not.
func probeACLEnabled(client *admin.Client, brokerAddr string, config connectionConfig) bool {
	if brokerAddr == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), minDuration(config.ConnectTimeout/2, 5*time.Second))
	defer cancel()
	_, err := client.GetBrokerClusterAclInfo(ctx, brokerAddr)
	return err == nil
}

// close releases the client and every loopback listener the proxies opened.
func (s *session) close() {
	if s.cancelLifetime != nil {
		s.cancelLifetime()
	}
	s.stopDashboardCollector()
	s.operations.Lock()
	defer s.operations.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	if s.client != nil {
		_ = s.client.Close()
	}
	if s.proxies != nil {
		s.proxies.Close()
	}
	s.client = nil
	s.proxies = nil
	s.connection.SecretKey = ""
	s.connection.AccessKey = ""
	if s.dashboard != nil {
		s.dashboard.mu.Lock()
		s.dashboard.queries = map[string]dashboardQuery{}
		s.dashboard.downloads = map[string]*dashboardDownload{}
		s.dashboard.mu.Unlock()
	}
}

func (s *session) requireClient() (*admin.Client, connectionConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || s.client == nil {
		return nil, connectionConfig{}, errors.New("connection is not open; reconnect from DBX")
	}
	return s.client, s.connection, nil
}

func (s *session) identity() (clusterName, brokerAddr string, proxies *proxyManager) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.clusterName, s.brokerAddr, s.proxies
}

func (s *session) writeAllowed() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.readOnly {
		return errors.New("connection is read-only")
	}
	return nil
}

func (s *session) info() connectionInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return connectionInfo{
		Name:         s.name,
		ServerAddr:   s.serverAddr,
		ClusterName:  s.clusterName,
		BrokerAddr:   s.brokerAddr,
		ACLEnabled:   s.aclEnabled,
		ReadOnly:     s.readOnly,
		PageSize:     s.pageSize,
		Capabilities: s.capabilitiesLocked(),
	}
}

// capabilitiesLocked mirrors the read/write split enforced by the plugin
// dispatcher, so the workbench can disable an action with the same reason the
// backend would reject it with.
func (s *session) capabilitiesLocked() map[string]operationCapability {
	supported := operationCapability{Supported: true}
	write := supported
	if s.readOnly {
		write = operationCapability{Supported: false, Reason: "connectionReadOnly"}
	}
	capabilities := make(map[string]operationCapability, len(readMethods)+len(writeMethods))
	for method := range readMethods {
		capabilities[method] = supported
	}
	for method := range writeMethods {
		capabilities[method] = write
	}
	return capabilities
}

// sessionMethods is the backend method table. It is a map rather than a switch
// so tests can assert that every exposed method is classified as either a read
// or a write - an unclassified method would bypass the read-only guard.
var sessionMethods = map[string]func(*session, map[string]any) (any, error){
	"mq_list_topics":                     (*session).listTopics,
	"mq_create_topic":                    (*session).createTopic,
	"mq_delete_topic":                    (*session).deleteTopic,
	"mq_update_partitions":               (*session).updatePartitions,
	"mq_get_topic_stats":                 (*session).getTopicStats,
	"mq_get_topic_route":                 (*session).getTopicRoute,
	"mq_get_topic_config":                (*session).getTopicConfig,
	"mq_alter_topic_config":              (*session).alterTopicConfig,
	"mq_skip_topic_accumulation":         (*session).skipTopicAccumulation,
	"mq_list_consumer_groups":            (*session).listConsumerGroups,
	"mq_describe_consumer_group":         (*session).describeConsumerGroup,
	"mq_delete_consumer_group":           (*session).deleteConsumerGroup,
	"mq_get_subscription_group_config":   (*session).getSubscriptionGroupConfig,
	"mq_alter_subscription_group_config": (*session).alterSubscriptionGroupConfig,
	"mq_reset_consumer_group_offsets":    (*session).resetConsumerGroupOffsets,
	"mq_get_consumer_lag":                (*session).getConsumerLag,
	"mq_list_producers":                  (*session).listProducers,
	"mq_peek_messages":                   (*session).peekMessages,
	"mq_view_message":                    (*session).viewMessage,
	"mq_query_message_by_key":            (*session).queryMessageByKey,
	"mq_query_message_by_topic":          (*session).queryMessageByTopic,
	"mq_query_message_trace":             (*session).queryMessageTrace,
	"mq_send_message":                    (*session).sendMessage,
	"mq_list_acls":                       (*session).listACLs,
	"mq_create_acls":                     (*session).createACLs,
	"mq_delete_acls":                     (*session).deleteACLs,
	"mq_describe_cluster":                (*session).describeCluster,
}

// dispatch routes a `mq_*` method to the ported agent implementation.
func (s *session) dispatch(method string, params map[string]any) (any, error) {
	handler, ok := sessionMethods[method]
	if !ok {
		return nil, fmt.Errorf("unsupported operation %s", method)
	}
	return handler(s, params)
}
