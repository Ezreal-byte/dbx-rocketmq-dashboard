package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	admin "github.com/amigoer/rocketmq-admin-go"
)

type dashboardRoute struct {
	Method string
	Write  bool
}

// This allowlist is also the source of truth for the coverage report.
var dashboardRoutes = map[string]dashboardRoute{
	"/cluster/list.query": {"GET", false}, "/cluster/brokerConfig.query": {"GET", false},
	"/topic/list.query": {"GET", false}, "/topic/list.queryTopicType": {"GET", false},
	"/topic/stats.query": {"GET", false}, "/topic/route.query": {"GET", false},
	"/topic/queryConsumerByTopic.query": {"GET", false}, "/topic/queryTopicConsumerInfo.query": {"GET", false},
	"/topic/examineTopicConfig.query": {"GET", false}, "/topic/createOrUpdate.do": {"POST", true},
	"/topic/deleteTopic.do": {"POST", true}, "/topic/deleteTopicByBroker.do": {"POST", true}, "/topic/sendTopicMessage.do": {"POST", true},
	"/consumer/groupList.query": {"GET", false}, "/consumer/group.refresh": {"GET", false}, "/consumer/group.refresh.all": {"GET", false},
	"/consumer/group.query": {"GET", false}, "/consumer/examineSubscriptionGroupConfig.query": {"GET", false},
	"/consumer/fetchBrokerNameList.query": {"GET", false}, "/consumer/queryTopicByConsumer.query": {"GET", false},
	"/consumer/consumerConnection.query": {"GET", false}, "/consumer/consumerRunningInfo.query": {"GET", false},
	"/consumer/createOrUpdate.do": {"POST", true}, "/consumer/deleteSubGroup.do": {"POST", true},
	"/consumer/resetOffset.do": {"POST", true}, "/consumer/skipAccumulate.do": {"POST", true},
	"/producer/producerConnection.query": {"GET", false},
	"/message/viewMessage.query":         {"GET", false}, "/message/queryMessageByTopicAndKey.query": {"GET", false},
	"/message/queryMessageByTopic.query": {"GET", false}, "/message/queryMessagePageByTopic.query": {"POST", false},
	"/message/consumeMessageDirectly.do": {"POST", true},
	"/messageTrace/viewMessage.query":    {"GET", false}, "/messageTrace/viewMessageTraceGraph.query": {"GET", false},
	"/dlqMessage/queryDlqMessageByConsumerGroup.query": {"POST", false}, "/dlqMessage/exportDlqMessage.do": {"GET", false},
	"/dlqMessage/batchResendDlqMessage.do": {"POST", true}, "/dlqMessage/batchExportDlqMessage.do": {"POST", false},
	"/acl/users.query": {"GET", false}, "/acl/acls.query": {"GET", false},
	"/acl/createUser.do": {"POST", true}, "/acl/updateUser.do": {"POST", true}, "/acl/deleteUser.do": {"DELETE", true},
	"/acl/createAcl.do": {"POST", true}, "/acl/updateAcl.do": {"POST", true}, "/acl/deleteAcl.do": {"DELETE", true},
	"/monitor/consumerMonitorConfigByGroupName.query": {"GET", false}, "/monitor/consumerMonitorConfig.query": {"GET", false},
	"/monitor/createOrUpdateConsumerMonitor.do": {"POST", true}, "/monitor/deleteConsumerMonitor.do": {"POST", true},
	"/dashboard/broker.query": {"GET", false}, "/dashboard/topic.query": {"GET", false}, "/dashboard/topicCurrent.query": {"GET", false},
	"/proxy/homePage.query": {"GET", false},
}

func dashboardResult(data any, err error) map[string]any {
	if err != nil {
		return map[string]any{"status": -1, "errMsg": err.Error(), "data": data}
	}
	return map[string]any{"status": 0, "errMsg": nil, "data": data}
}

func (s *session) dashboardRequest(request map[string]any) map[string]any {
	s.operations.RLock()
	defer s.operations.RUnlock()
	path := stringValue(request, "path")
	route, ok := dashboardRoutes[path]
	if !ok {
		return dashboardResult(nil, fmt.Errorf("unknown Dashboard endpoint: %s", path))
	}
	if strings.ToUpper(stringValue(request, "method")) != route.Method {
		return dashboardResult(nil, fmt.Errorf("invalid method for %s", path))
	}
	if route.Write {
		if err := s.writeAllowed(); err != nil {
			return dashboardResult(nil, err)
		}
	}
	p := map[string]any{}
	for k, v := range nestedMap(request, "query") {
		p[k] = v
	}
	for k, v := range nestedMap(request, "body") {
		p[k] = v
	}
	if rows, ok := request["body"].([]any); ok {
		p["items"] = rows
	}
	// System prefixes are display markers, not part of a resource's identity.
	for _, k := range []string{"topic", "consumerGroup"} {
		if v, ok := p[k].(string); ok {
			p[k] = strings.TrimPrefix(v, "%SYS%")
		}
	}
	client, config, err := s.requireClient()
	if err != nil {
		return dashboardResult(nil, err)
	}
	parent := s.lifetime
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, 55*time.Second)
	defer cancel()
	var data any
	switch {
	case strings.HasPrefix(path, "/topic/"):
		data, err = s.dashboardTopic(ctx, client, path, p)
	case strings.HasPrefix(path, "/consumer/"):
		data, err = s.dashboardConsumer(ctx, client, path, p)
	case strings.HasPrefix(path, "/message") || strings.HasPrefix(path, "/dlqMessage/"):
		data, err = s.dashboardMessage(ctx, client, path, p)
	case strings.HasPrefix(path, "/acl/"):
		data, err = s.dashboardACL(ctx, client, path, p)
	case strings.HasPrefix(path, "/dashboard/") || strings.HasPrefix(path, "/monitor/"):
		data, err = s.dashboardMetrics(ctx, client, path, p)
	case path == "/cluster/list.query":
		data, err = s.dashboardCluster(ctx, client)
	case path == "/cluster/brokerConfig.query":
		var addr string
		addr, err = s.dashboardAddress(ctx, client, stringValue(p, "brokerAddr"))
		if err == nil {
			data, err = client.GetBrokerConfig(ctx, addr)
		}
	case path == "/producer/producerConnection.query":
		data, err = s.dashboardProducer(ctx, client, stringValue(p, "producerGroup"), stringValue(p, "topic"))
	case path == "/proxy/homePage.query":
		data = map[string]any{"proxyAddrList": splitAddresses(config.ProxyAddr), "currentProxyAddr": config.ProxyAddr}
	default:
		err = fmt.Errorf("unhandled Dashboard endpoint: %s", path)
	}
	return dashboardResult(data, err)
}

func jsonObject(value any) map[string]any {
	b, _ := json.Marshal(value)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	return m
}

func (s *session) dashboardAddress(ctx context.Context, c *admin.Client, wanted string) (string, error) {
	info, err := c.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		return "", err
	}
	for _, b := range info.BrokerAddrTable {
		for _, addr := range b.BrokerAddrs {
			if addr == wanted || s.proxies.OriginalForLocal(addr) == wanted {
				return addr, nil
			}
		}
	}
	return "", fmt.Errorf("broker address does not belong to this connection: %s", wanted)
}

func (s *session) dashboardBrokers(ctx context.Context, c *admin.Client, p map[string]any) (map[string]string, error) {
	info, err := c.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		return nil, err
	}
	names := stringSlice(p["brokerNameList"])
	if name := stringValue(p, "brokerName"); name != "" {
		names = append(names, name)
	}
	clusters := stringSlice(p["clusterNameList"])
	if name := stringValue(p, "clusterName"); name != "" {
		clusters = append(clusters, name)
	}
	selected := map[string]bool{}
	for _, name := range names {
		if info.BrokerAddrTable[name] == nil {
			return nil, fmt.Errorf("unknown broker %s", name)
		}
		selected[name] = true
	}
	for _, name := range clusters {
		list, ok := info.ClusterAddrTable[name]
		if !ok {
			return nil, fmt.Errorf("unknown cluster %s", name)
		}
		for _, b := range list {
			selected[b] = true
		}
	}
	result := map[string]string{}
	for name, b := range info.BrokerAddrTable {
		if len(selected) > 0 && !selected[name] {
			continue
		}
		if addr := b.BrokerAddrs["0"]; addr != "" {
			result[name] = addr
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no master broker available")
	}
	return result, nil
}

func (s *session) dashboardCluster(ctx context.Context, c *admin.Client) (any, error) {
	info, err := c.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		return nil, err
	}
	stats := map[string]any{}
	for name, b := range info.BrokerAddrTable {
		byID := map[string]any{}
		for id, addr := range b.BrokerAddrs {
			kv, e := c.FetchBrokerRuntimeStats(ctx, addr)
			if e != nil {
				return nil, fmt.Errorf("broker %s: %w", name, e)
			}
			byID[id] = kv.Table
			if original := s.proxies.OriginalForLocal(addr); original != "" {
				b.BrokerAddrs[id] = original
			}
		}
		stats[name] = byID
	}
	return map[string]any{"clusterInfo": info, "brokerServer": stats, "messageTypes": map[string]string{"NORMAL": "MESSAGE_TYPE_NORMAL", "FIFO": "MESSAGE_TYPE_FIFO", "DELAY": "MESSAGE_TYPE_DELAY", "TRANSACTION": "MESSAGE_TYPE_TRANSACTION", "UNSPECIFIED": "MESSAGE_TYPE_UNSPECIFIED"}}, nil
}

func sortedMapKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
func mutateBrokers(brokers map[string]string, action func(string) error) (any, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no writable master Broker found")
	}
	done := []string{}
	failed := []string{}
	for _, name := range sortedMapKeys(brokers) {
		if err := action(brokers[name]); err != nil {
			failed = append(failed, name+": "+err.Error())
		} else {
			done = append(done, name)
		}
	}
	if len(failed) > 0 {
		return nil, fmt.Errorf("completed brokers %v; failed brokers: %s", done, strings.Join(failed, "; "))
	}
	return true, nil
}
