package main

import (
	"context"
	"encoding/json"
	"fmt"
	admin "github.com/amigoer/rocketmq-admin-go"
	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
	"strconv"
	"strings"
	"time"
)

func studioTPS(value string) float64 {
	fields := strings.Fields(value)
	if len(fields) == 0 {
		return 0
	}
	n, _ := strconv.ParseFloat(fields[0], 64)
	return n
}
func studioVersion(v int) string {
	if v >= 0 && v < len(dashboardVersionNames) {
		return dashboardVersionNames[v]
	}
	return fmt.Sprintf("UNKNOWN(%d)", v)
}
func (s *session) studioClusters(ctx context.Context, c *admin.Client) ([]any, error) {
	info, e := c.ExamineBrokerClusterInfo(ctx)
	if e != nil {
		return nil, e
	}
	topics, complete := s.collectTopicConfigs(ctx)
	if !complete {
		return nil, fmt.Errorf("topic inventory incomplete")
	}
	groups, e := s.collectSubscriptionGroupConfigs(ctx)
	if e != nil {
		return nil, e
	}
	nt, ng := 0, 0
	for n, cfg := range topics {
		kind := classifyTopicMessageType(n, s.clusterName, cfg.Attributes)
		if kind != "SYSTEM" && kind != "RETRY" && kind != "DLQ" {
			nt++
		}
	}
	for n, cfg := range groups {
		if classifyConsumerGroup(n, cfg) != "SYSTEM" {
			ng++
		}
	}
	rows := []any{}
	for _, name := range sortedKeys(info.ClusterAddrTable) {
		brokers := []any{}
		config := map[string]any{}
		version := ""
		for _, broker := range info.ClusterAddrTable[name] {
			b := info.BrokerAddrTable[broker]
			if b == nil {
				continue
			}
			for _, id := range sortedMapKeys(b.BrokerAddrs) {
				addr := b.BrokerAddrs[id]
				stats, e := c.FetchBrokerRuntimeStats(ctx, addr)
				if e != nil {
					return nil, fmt.Errorf("broker %s: %w", broker, e)
				}
				version = stats.Table["brokerVersionDesc"]
				brokers = append(brokers, map[string]any{"name": broker, "addr": valueOrDefault(s.proxies.OriginalForLocal(addr), addr), "status": "healthy", "tpsIn": studioTPS(stats.Table["putTps"]), "tpsOut": studioTPS(stats.Table["getTransferredTps"]), "diskUsage": floatValue(stats.Table["commitLogDiskRatio"]) * 100, "version": version, "runtimeStatsAvailable": true})
				if len(config) == 0 {
					props, e := c.GetBrokerConfig(ctx, addr)
					if e != nil {
						return nil, e
					}
					config = studioClusterConfig(props)
				}
			}
		}
		ns := []any{}
		for _, addr := range s.connection.NameServers {
			ns = append(ns, map[string]any{"addr": addr, "status": "healthy"})
		}
		proxies := []any{}
		for _, addr := range splitAddresses(s.connection.ProxyAddr) {
			proxies = append(proxies, map[string]any{"addr": addr, "status": "UNKNOWN", "connections": nil, "grpcPort": nil, "remotingPort": nil})
		}
		rows = append(rows, map[string]any{"id": name, "name": name, "nsClusterName": name, "type": "V4_DIRECT", "endpoint": s.serverAddr, "status": "healthy", "version": version, "brokers": brokers, "proxies": proxies, "nameServers": ns, "config": config, "topicCount": nt, "groupCount": ng, "tpsHistory": []any{}})
	}
	return rows, nil
}

var studioConfigFields = map[string]string{"flushDiskType": "flushDiskType", "autoCreateTopicEnable": "autoCreateTopicEnable", "autoCreateSubscriptionGroup": "autoCreateSubscriptionGroup", "maxMessageSize": "maxMessageSize", "msgTraceTopicName": "msgTraceTopicName", "fileReservedTime": "fileReservedTime", "writeQueueNums": "defaultTopicQueueNums", "readQueueNums": "defaultTopicQueueNums", "brokerPermission": "brokerPermission", "deleteWhen": "deleteWhen"}

func studioClusterConfig(props map[string]string) map[string]any {
	m := map[string]any{}
	for field, key := range studioConfigFields {
		v := props[key]
		switch field {
		case "autoCreateTopicEnable", "autoCreateSubscriptionGroup":
			m[field] = v == "true"
		case "maxMessageSize", "fileReservedTime", "writeQueueNums", "readQueueNums", "brokerPermission":
			n, e := strconv.Atoi(v)
			if e == nil {
				m[field] = n
			} else {
				m[field] = nil
			}
		default:
			m[field] = v
		}
	}
	return m
}
func (s *session) studioTopology(ctx context.Context, c *admin.Client, path string, p map[string]any) (any, error) {
	switch path {
	case "/instances/:id/capabilities":
		if stringValue(p, "id") != "1" && stringValue(p, "id") != valueOrDefault(s.name, s.clusterName) {
			return nil, fmt.Errorf("unknown instance")
		}
		return map[string]any{"instanceId": "1", "vendor": "APACHE", "accessType": "DIRECT", "capabilities": []string{"TOPIC_MANAGEMENT", "CONSUMER_GROUP_MANAGEMENT", "MESSAGE_QUERY", "MESSAGE_TRACE", "MESSAGE_SEND", "ACL_MANAGEMENT", "DLQ_MANAGEMENT"}}, nil
	case "/instances":
		topics, complete := s.collectTopicConfigs(ctx)
		if !complete {
			return nil, fmt.Errorf("topic inventory incomplete")
		}
		groups, e := s.collectSubscriptionGroupConfigs(ctx)
		if e != nil {
			return nil, e
		}
		kind := "DIRECT"
		if s.connection.ProxyAddr != "" {
			kind = "PROXY_LOCAL"
		}
		if filter := stringValue(p, "type"); filter != "" && filter != kind {
			return []any{}, nil
		}
		if !studioMatches(s.name, p) {
			return []any{}, nil
		}
		return []any{map[string]any{"id": 1, "name": valueOrDefault(s.name, s.clusterName), "remark": "DBX", "type": kind, "vendor": "APACHE", "endpoint": s.serverAddr, "topicCount": len(topics), "consumerGroupCount": len(groups), "resourceCountsAvailable": true, "gmtCreate": nil, "gmtModified": nil}}, nil
	case "/clusters", "/clusters/registry", "/clusters/:id", "/dashboard":
		clusters, e := s.studioClusters(ctx, c)
		if e != nil {
			return nil, e
		}
		if path == "/clusters/:id" {
			for _, v := range clusters {
				m := v.(map[string]any)
				if m["id"] == p["id"] {
					return m, nil
				}
			}
			return nil, fmt.Errorf("unknown cluster")
		}
		if path != "/dashboard" {
			return clusters, nil
		}
		var in, out float64
		nb, np, ns, nt, ng := 0, 0, 0, 0, 0
		overview := []any{}
		for _, v := range clusters {
			m := jsonObject(v)
			localIn, localOut := 0.0, 0.0
			for _, b := range studioRows(m["brokers"]) {
				nb++
				localIn += floatValue(b["tpsIn"])
				localOut += floatValue(b["tpsOut"])
			}
			in += localIn
			out += localOut
			np += len(studioRows(m["proxies"]))
			ns += len(studioRows(m["nameServers"]))
			nt += intValue(m, 0, "topicCount")
			ng += intValue(m, 0, "groupCount")
			overview = append(overview, map[string]any{"id": m["id"], "name": m["name"], "type": m["type"], "status": m["status"], "version": m["version"], "brokers": len(studioRows(m["brokers"])), "proxies": len(studioRows(m["proxies"])), "topics": m["topicCount"], "groups": m["groupCount"], "tpsIn": localIn, "tpsOut": localOut, "throughput": []any{}})
		}
		return map[string]any{"stats": map[string]any{"totalClusters": len(clusters), "healthyClusters": len(clusters), "totalBrokers": nb, "totalProxies": np, "totalNameServers": ns, "totalTopics": nt, "totalConsumerGroups": ng, "totalMessagesToday": nil, "messagesPerSecond": in, "tpsIn": in, "tpsOut": out}, "clusters": overview}, nil
	case "/producer/groups", "/producer/connection", "/clients":
		return s.studioClients(ctx, c, path, p)
	case "/nameservers":
		rows := []any{}
		for i, addr := range s.connection.NameServers {
			rows = append(rows, map[string]any{"id": i + 1, "name": s.clusterName, "namesrvAddr": addr, "description": "DBX", "status": "healthy"})
		}
		return rows, nil
	case "/clusters/:id/broker-config-diff", "/clusters/config/preview", "/clusters/config/update":
		return s.studioBrokerConfig(ctx, c, path, p)
	case "/nameservers/config-diff":
		return s.studioNameserverDiff(ctx, c, p)
	case "/metrics/profiles":
		return []any{map[string]any{"id": "dbx-remoting", "name": "RocketMQ Broker", "description": "DBX connection history", "metrics": []any{map[string]any{"semanticMetric": "broker_get_tps", "name": "Broker Get TPS", "unit": "msg/s", "prometheusMetric": "", "promql": "dbx_broker_get_tps", "labels": []string{"broker"}}}}}, nil
	case "/settings/datasources":
		return []any{}, nil
	case "/metrics/query":
		return s.studioMetricQuery(p)
	}
	return nil, fmt.Errorf("unhandled Studio endpoint: %s", path)
}
func (s *session) studioClients(ctx context.Context, c *admin.Client, path string, p map[string]any) (any, error) {
	brokers, e := s.dashboardBrokers(ctx, c, map[string]any{})
	if e != nil {
		return nil, e
	}
	rows := []any{}
	producerRows := []any{}
	groups := map[string]bool{}
	seen := map[string]bool{}
	for _, broker := range sortedMapKeys(brokers) {
		response, e := invokeRemotingWithClient(ctx, brokers[broker], remoting.NewRequest(remoting.GetProducerInfo, nil))
		if e != nil {
			return nil, e
		}
		var table struct {
			Data map[string][]map[string]any `json:"data"`
		}
		if e = json.Unmarshal(response.Body, &table); e != nil {
			return nil, e
		}
		for _, g := range sortedKeys(table.Data) {
			groups[g] = true
			for _, v := range table.Data[g] {
				id := stringValue(v, "clientId")
				addr := stringValue(v, "clientAddr", "remoteIP")
				key := g + "|" + id + "|" + addr
				if seen[key] {
					continue
				}
				seen[key] = true
				version := studioVersion(intValue(v, -1, "version"))
				language := v["language"]
				row := map[string]any{"clientId": id, "type": "Producer", "groupOrTopic": g, "protocol": "Remoting", "address": addr, "language": language, "version": version, "connectedAt": nil, "clusterName": s.clusterName}
				rows = append(rows, row)
				if wanted := stringValue(p, "producerGroup"); wanted == "" || wanted == g {
					producerRows = append(producerRows, map[string]any{"clientId": id, "clientAddr": addr, "producerGroup": g, "language": language, "versionDesc": version})
				}
			}
		}
	}
	if path == "/producer/groups" {
		names := []string{}
		for _, n := range sortedKeys(groups) {
			if q := stringValue(p, "query"); q == "" || strings.Contains(strings.ToLower(n), strings.ToLower(q)) {
				names = append(names, n)
			}
		}
		return names, nil
	}
	if path == "/producer/connection" {
		return map[string]any{"connectionSet": producerRows, "complete": true, "failedBrokers": []string{}, "failedProducerGroups": []string{}}, nil
	}
	configs, e := s.collectSubscriptionGroupConfigs(ctx)
	if e != nil {
		return nil, e
	}
	for _, g := range sortedKeys(configs) {
		conn, e := s.dashboardConnection(ctx, c, g, "")
		if rocketMQCode(e, 206) {
			continue
		}
		if e != nil {
			return nil, e
		}
		for _, v := range conn.ConnectionSet {
			rows = append(rows, map[string]any{"clientId": v.ClientId, "type": "Consumer", "groupOrTopic": g, "protocol": "Remoting", "address": v.ClientAddr, "language": v.Language, "version": studioVersion(v.Version), "connectedAt": nil, "clusterName": s.clusterName})
		}
	}
	filtered := []any{}
	for _, v := range rows {
		m := v.(map[string]any)
		if kind := stringValue(p, "type"); kind == "" || strings.EqualFold(kind, stringValue(m, "type")) {
			filtered = append(filtered, m)
		}
	}
	return filtered, nil
}
func (s *session) studioBrokerConfig(ctx context.Context, c *admin.Client, path string, p map[string]any) (any, error) {
	cluster := stringValue(p, "id")
	brokers, e := s.dashboardBrokers(ctx, c, map[string]any{"clusterName": cluster})
	if e != nil {
		return nil, e
	}
	props := map[string]map[string]string{}
	targets := []any{}
	for _, name := range sortedMapKeys(brokers) {
		value, e := c.GetBrokerConfig(ctx, brokers[name])
		if e != nil {
			return nil, e
		}
		props[name] = value
		targets = append(targets, map[string]any{"name": name, "address": valueOrDefault(s.proxies.OriginalForLocal(brokers[name]), brokers[name]), "reachable": true})
	}
	if strings.HasSuffix(path, "broker-config-diff") {
		diffs := []any{}
		for _, field := range sortedMapKeys(studioConfigFields) {
			key := studioConfigFields[field]
			values := []any{}
			distinct := map[string]bool{}
			for _, name := range sortedKeys(props) {
				value, present := props[name][key]
				distinct[fmt.Sprint(present)+value] = true
				values = append(values, map[string]any{"brokerName": name, "address": valueOrDefault(s.proxies.OriginalForLocal(brokers[name]), brokers[name]), "configured": present, "value": value})
			}
			if len(distinct) > 1 {
				diffs = append(diffs, map[string]any{"field": field, "brokerProperty": key, "values": values})
			}
		}
		return map[string]any{"cluster": cluster, "complete": true, "driftDetected": len(diffs) > 0, "brokerCount": len(brokers), "reachableBrokerCount": len(brokers), "comparedFields": sortedMapKeys(studioConfigFields), "brokers": targets, "differences": diffs}, nil
	}
	current := studioClusterConfig(props[sortedKeys(props)[0]])
	proposed := jsonObject(current)
	changes := []any{}
	update := map[string]string{}
	for _, field := range sortedMapKeys(studioConfigFields) {
		if value, ok := p[field]; ok {
			key := studioConfigFields[field]
			v := fmt.Sprint(value)
			if existing, ok := update[key]; ok && existing != v {
				return nil, fmt.Errorf("read/write queues use the same Broker property")
			}
			update[key] = v
			proposed[field] = value
			if fmt.Sprint(current[field]) != v {
				changes = append(changes, map[string]any{"field": field, "currentValue": fmt.Sprint(current[field]), "proposedValue": v, "brokerProperty": key})
			}
		}
	}
	clusters, e := s.studioClusters(ctx, c)
	if e != nil {
		return nil, e
	}
	var clusterInfo any
	for _, row := range clusters {
		if row.(map[string]any)["id"] == cluster {
			clusterInfo = row
		}
	}
	if path == "/clusters/config/preview" {
		return map[string]any{"cluster": clusterInfo, "currentConfig": current, "proposedConfig": proposed, "targetBrokers": targets, "brokerProperties": update, "changes": changes, "changed": len(changes) > 0}, nil
	}
	if len(update) == 0 {
		return nil, fmt.Errorf("no registered Broker settings supplied")
	}
	done := []string{}
	fail := []any{}
	for _, name := range sortedMapKeys(brokers) {
		addr := brokers[name]
		e := c.UpdateBrokerConfig(ctx, addr, update)
		original := valueOrDefault(s.proxies.OriginalForLocal(addr), addr)
		if e != nil {
			fail = append(fail, map[string]any{"address": original, "message": e.Error()})
		} else {
			done = append(done, original)
		}
	}
	status := "SUCCESS"
	if len(fail) > 0 {
		status = "PARTIAL"
		if len(done) == 0 {
			status = "FAILED"
		}
	}
	return map[string]any{"cluster": clusterInfo, "status": status, "successfulBrokers": done, "failedBrokers": fail}, nil
}
func (s *session) studioNameserverDiff(ctx context.Context, c *admin.Client, p map[string]any) (any, error) {
	values := map[string]map[string]string{}
	keys := map[string]bool{}
	nodes := []any{}
	for _, addr := range c.GetNameServerAddressList() {
		r, e := invokeRemotingWithClient(ctx, addr, remoting.NewRequest(319, nil))
		if e != nil {
			return nil, e
		}
		props := map[string]string{}
		for _, line := range strings.Split(string(r.Body), "\n") {
			pair := strings.SplitN(strings.TrimSpace(line), "=", 2)
			if len(pair) == 2 {
				props[pair[0]] = pair[1]
				keys[pair[0]] = true
			}
		}
		original := valueOrDefault(s.proxies.OriginalForLocal(addr), addr)
		values[original] = props
		nodes = append(nodes, map[string]any{"address": original, "reachable": true})
	}
	diff := []any{}
	for _, key := range sortedKeys(keys) {
		distinct := map[string]bool{}
		rows := []any{}
		for _, addr := range sortedKeys(values) {
			v, ok := values[addr][key]
			distinct[fmt.Sprint(ok)+v] = true
			rows = append(rows, map[string]any{"address": addr, "configured": ok, "value": v})
		}
		if len(distinct) > 1 {
			diff = append(diff, map[string]any{"key": key, "values": rows})
		}
	}
	return map[string]any{"cluster": stringValue(p, "clusterId"), "complete": true, "driftDetected": len(diff) > 0, "nodeCount": len(nodes), "reachableNodeCount": len(nodes), "comparedKeys": sortedKeys(keys), "nodes": nodes, "differences": diff}, nil
}
func (s *session) studioMetricQuery(p map[string]any) (any, error) {
	if stringValue(p, "metric") != "dbx_broker_get_tps" {
		return nil, fmt.Errorf("select a DBX history metric")
	}
	begin, end := int64Value(p, 0, "start"), int64Value(p, time.Now().Unix(), "end")
	if begin > 1e12 {
		begin /= 1000
	}
	if end > 1e12 {
		end /= 1000
	}
	if begin > end || end-begin > 7*86400 {
		return nil, fmt.Errorf("history query must span at most 7 days")
	}
	s.dashboard.mu.Lock()
	defer s.dashboard.mu.Unlock()
	series := map[string][]any{}
	for day := time.Unix(begin, 0); !day.After(time.Unix(end, 0)); day = day.AddDate(0, 0, 1) {
		h, e := s.dashboard.loadHistoryLocked(day.Format("2006-01-02"))
		if e != nil {
			return nil, e
		}
		for broker, points := range h.Broker {
			for _, point := range points {
				parts := strings.Split(point, ",")
				if len(parts) < 2 {
					continue
				}
				ms, _ := strconv.ParseInt(parts[0], 10, 64)
				if ms/1000 >= begin && ms/1000 <= end {
					series[broker] = append(series[broker], map[string]any{"timestamp": float64(ms) / 1000, "value": parts[1]})
				}
			}
		}
	}
	rows := []any{}
	for _, broker := range sortedKeys(series) {
		rows = append(rows, map[string]any{"labels": map[string]string{"broker": broker}, "values": series[broker], "histograms": []any{}})
	}
	return map[string]any{"resultType": "matrix", "series": rows, "warnings": []string{}}, nil
}
