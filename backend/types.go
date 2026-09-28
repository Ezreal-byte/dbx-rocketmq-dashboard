package main

const (
	pluginID      = "io.dbx.rocketmq-dashboard-console"
	pluginVersion = "0.3.0"
)

// externalConfig mirrors `external_config` of the connection-provider fields
// declared with `binding: "config"`. Declared as its own struct so unknown keys
// written by other versions are ignored instead of failing the request.
type externalConfig struct {
	UseTLS        bool   `json:"use_tls"`
	VIPChannel    bool   `json:"vip_channel"`
	ProxyAddr     string `json:"proxy_addr"`
	NamesrvAddr   string `json:"namesrv_addr"`
	ClusterName   string `json:"cluster_name"`
	TLSSkipVerify bool   `json:"tls_skip_verify"`
	ReadOnly      bool   `json:"read_only"`
	PageSize      int    `json:"page_size"`
}

type transportLayer struct {
	Type    string `json:"type"`
	Enabled *bool  `json:"enabled"`
}

// connection is the hydrated connection config. Secrets declared with
// `binding: "secret"` only reach the backend on lifecycle requests, inside
// `connection_secrets`; they must never be logged or echoed back.
type connection struct {
	ID                string            `json:"id"`
	Name              string            `json:"name"`
	Host              string            `json:"host"`
	Port              int               `json:"port"`
	Database          string            `json:"database"`
	ReadOnly          bool              `json:"read_only"`
	Config            externalConfig    `json:"external_config"`
	ConnectionSecrets map[string]string `json:"connection_secrets"`
	TransportLayers   []transportLayer  `json:"transport_layers"`
}

// runtimeProxy is the SOCKS5 route DBX hands to `proxy_route` providers so that
// every NameServer and broker address can be dialed through one transport
// layer. Credentials ride the same encrypted channel as connection secrets.
type runtimeProxy struct {
	Type     string `json:"type"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
}

// runtimeEndpoint is the endpoint DBX resolved for this connection. For a
// SOCKS5-routed provider the logical host/port may be empty, because the plugin
// is expected to dial the advertised addresses through `proxy` instead.
type runtimeEndpoint struct {
	Host  string        `json:"host"`
	Port  int           `json:"port"`
	Proxy *runtimeProxy `json:"proxy"`
}

type lifecycleParams struct {
	Connection connection      `json:"connection"`
	Runtime    runtimeEndpoint `json:"runtime"`
}

type operationCapability struct {
	Supported bool   `json:"supported"`
	Reason    string `json:"reason,omitempty"`
}

// connectionInfo is what `rocketmq/info` returns and what the workbench uses to
// decide which tabs and actions to offer.
type connectionInfo struct {
	Name         string                         `json:"name"`
	ServerAddr   string                         `json:"serverAddr"`
	ClusterName  string                         `json:"clusterName"`
	BrokerAddr   string                         `json:"brokerAddr"`
	ACLEnabled   bool                           `json:"aclEnabled"`
	ReadOnly     bool                           `json:"readOnly"`
	PageSize     int                            `json:"pageSize"`
	Capabilities map[string]operationCapability `json:"capabilities"`
}

// readMethods are the backend methods the workbench may call on a read-only
// connection. Anything not listed here is treated as a write, so a new method
// fails closed instead of silently bypassing the read-only guard.
var readMethods = map[string]struct{}{
	"mq_list_topics":                   {},
	"mq_get_topic_stats":               {},
	"mq_get_topic_route":               {},
	"mq_get_topic_config":              {},
	"mq_list_consumer_groups":          {},
	"mq_describe_consumer_group":       {},
	"mq_get_subscription_group_config": {},
	"mq_get_consumer_lag":              {},
	"mq_list_producers":                {},
	"mq_peek_messages":                 {},
	"mq_view_message":                  {},
	"mq_query_message_by_key":          {},
	"mq_query_message_by_topic":        {},
	"mq_query_message_trace":           {},
	"mq_list_acls":                     {},
	"mq_describe_cluster":              {},
}

// writeMethods are rejected with `connectionReadOnly` when the connection is
// read-only. Keeping the list explicit makes the capability snapshot and the
// guard impossible to drift apart.
var writeMethods = map[string]struct{}{
	"mq_create_topic":                    {},
	"mq_delete_topic":                    {},
	"mq_update_partitions":               {},
	"mq_alter_topic_config":              {},
	"mq_skip_topic_accumulation":         {},
	"mq_delete_consumer_group":           {},
	"mq_alter_subscription_group_config": {},
	"mq_reset_consumer_group_offsets":    {},
	"mq_send_message":                    {},
	"mq_create_acls":                     {},
	"mq_delete_acls":                     {},
}

func isReadMethod(method string) bool {
	_, ok := readMethods[method]
	return ok
}

func isWriteMethod(method string) bool {
	_, ok := writeMethods[method]
	return ok
}
