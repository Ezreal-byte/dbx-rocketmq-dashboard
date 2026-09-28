package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	admin "github.com/amigoer/rocketmq-admin-go"
	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
)

func (s *session) dashboardConsumer(ctx context.Context, c *admin.Client, path string, p map[string]any) (any, error) {
	group := stringValue(p, "consumerGroup")
	switch path {
	case "/consumer/groupList.query", "/consumer/group.refresh.all":
		configs, err := s.collectSubscriptionGroupConfigs(ctx)
		if err != nil {
			return nil, err
		}
		rows := []any{}
		for _, name := range sortedKeys(configs) {
			row, e := s.dashboardGroup(ctx, c, name, stringValue(p, "address"))
			if e != nil {
				return nil, e
			}
			kind := classifyConsumerGroup(name, configs[name])
			row["subGroupType"] = kind
			if kind == "SYSTEM" && !boolValue(p, false, "skipSysGroup") {
				row["group"] = "%SYS%" + name
			}
			rows = append(rows, row)
		}
		return rows, nil
	case "/consumer/group.query", "/consumer/group.refresh":
		return s.dashboardGroup(ctx, c, group, stringValue(p, "address"))
	case "/consumer/consumerConnection.query":
		conn, err := s.dashboardConnection(ctx, c, group, stringValue(p, "address"))
		if err != nil {
			return nil, err
		}
		return conn, nil
	case "/consumer/consumerRunningInfo.query":
		return s.dashboardClientControl(ctx, c, group, stringValue(p, "clientId"), remoting.GetConsumerRunningInfo, map[string]string{"jstackEnable": fmt.Sprint(boolValue(p, false, "jstack"))})
	case "/consumer/queryTopicByConsumer.query":
		return s.dashboardConsumeStats(ctx, c, group, "")
	case "/consumer/examineSubscriptionGroupConfig.query", "/consumer/fetchBrokerNameList.query":
		brokers, err := s.dashboardBrokers(ctx, c, map[string]any{})
		if err != nil {
			return nil, err
		}
		rows := []any{}
		names := []string{}
		for _, name := range sortedMapKeys(brokers) {
			configs, e := fetchSubscriptionGroupConfigs(ctx, brokers[name])
			if e != nil {
				return nil, e
			}
			if cfg := configs[group]; cfg != nil {
				rows = append(rows, map[string]any{"brokerNameList": []string{name}, "subscriptionGroupConfig": cfg})
				names = append(names, name)
			}
		}
		if path == "/consumer/fetchBrokerNameList.query" {
			return names, nil
		}
		return rows, nil
	case "/consumer/createOrUpdate.do":
		if len(stringSlice(p["brokerNameList"]))+len(stringSlice(p["clusterNameList"])) == 0 {
			return nil, fmt.Errorf("select at least one cluster or broker")
		}
		cfg := subscriptionGroupConfig{ConsumeEnable: true, ConsumeBroadcastEnable: true, RetryQueueNums: 1, RetryMaxTimes: 16}
		raw, _ := json.Marshal(p["subscriptionGroupConfig"])
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, err
		}
		if cfg.GroupName == "" {
			return nil, fmt.Errorf("groupName is required")
		}
		brokers, err := s.dashboardBrokers(ctx, c, p)
		if err != nil {
			return nil, err
		}
		return mutateBrokers(brokers, func(addr string) error { return writeSubscriptionGroupConfig(ctx, addr, &cfg) })
	case "/consumer/deleteSubGroup.do":
		group = stringValue(p, "groupName", "consumerGroup")
		if group == "" {
			return nil, fmt.Errorf("groupName is required")
		}
		brokers, err := s.dashboardBrokers(ctx, c, p)
		if err != nil {
			return nil, err
		}
		all, err := s.dashboardBrokers(ctx, c, map[string]any{})
		if err != nil {
			return nil, err
		}
		deleteInNS := true
		for name, addr := range all {
			configs, e := fetchSubscriptionGroupConfigs(ctx, addr)
			if e != nil {
				return nil, e
			}
			if configs[group] != nil && brokers[name] == "" {
				deleteInNS = false
			}
		}
		result, err := mutateBrokers(brokers, func(addr string) error {
			_, e := invokeRemotingWithClient(ctx, addr, remoting.NewRequest(remoting.DeleteSubscriptionGroup, map[string]string{"groupName": group, "cleanOffset": "true"}))
			if e != nil {
				return e
			}
			for _, prefix := range []string{"%RETRY%", "%DLQ%"} {
				if e = c.DeleteTopicInBroker(ctx, addr, prefix+group); e != nil {
					return fmt.Errorf("group deleted; %s cleanup: %w", prefix, e)
				}
			}
			return nil
		})
		if err != nil {
			return result, err
		}
		if deleteInNS {
			for _, prefix := range []string{"%RETRY%", "%DLQ%"} {
				if err = s.dashboardDeleteTopicNS(ctx, c, prefix+group); err != nil {
					return nil, fmt.Errorf("group deleted; NameServer cleanup: %w", err)
				}
			}
		}
		return true, nil
	case "/consumer/resetOffset.do", "/consumer/skipAccumulate.do":
		topic := stringValue(p, "topic")
		groups := stringSlice(p["consumerGroupList"])
		if topic == "" || len(groups) == 0 {
			return nil, fmt.Errorf("topic and consumerGroupList are required")
		}
		result := map[string]any{}
		for _, g := range groups {
			rows, err := s.dashboardReset(ctx, c, topic, g, int64Value(p, -1, "resetTime"), boolValue(p, false, "force"))
			if err != nil {
				result[g] = map[string]any{"status": false, "errMsg": err.Error(), "rollbackStatsList": []any{}}
				continue
			}
			result[g] = map[string]any{"status": true, "rollbackStatsList": rows}
		}
		return result, nil
	}
	return nil, fmt.Errorf("unknown consumer operation %s", path)
}

func (s *session) dashboardGroup(ctx context.Context, c *admin.Client, group string, proxy ...string) (map[string]any, error) {
	if group == "" {
		return nil, fmt.Errorf("consumerGroup is required")
	}
	result := map[string]any{"group": group, "count": 0, "diffTotal": int64(0), "consumeTps": float64(0), "updateTime": time.Now().Format("2006-01-02 15:04:05"), "address": []string{}}
	brokers, err := s.dashboardBrokers(ctx, c, map[string]any{})
	if err != nil {
		return nil, err
	}
	addresses := []string{}
	var total int64
	var tps float64
	for _, addr := range brokers {
		configs, e := fetchSubscriptionGroupConfigs(ctx, addr)
		if e != nil {
			return nil, e
		}
		if configs[group] == nil {
			continue
		}
		original := s.proxies.OriginalForLocal(addr)
		addresses = append(addresses, valueOrDefault(original, addr))
		response, e := invokeRemotingAllowCodes(ctx, addr, s.connection.ConnectTimeout, remoting.NewRequest(remoting.GetConsumeStats, map[string]string{"consumerGroup": group}), 0, 203, 207)
		if e != nil {
			return nil, e
		}
		if response.Code != 0 {
			continue
		}
		stats, e := decodeConsumeStats(response.Body)
		if e != nil {
			return nil, e
		}
		tps += stats.ConsumeTps
		for _, o := range stats.OffsetTable {
			total += o.BrokerOffset - o.ConsumerOffset
		}
	}
	result["address"] = addresses
	result["diffTotal"] = total
	result["consumeTps"] = tps
	address := ""
	if len(proxy) > 0 {
		address = proxy[0]
	}
	conn, e := s.dashboardConnection(ctx, c, group, address)
	if e != nil && !rocketMQCode(e, 206) {
		return nil, e
	}
	if e == nil {
		result["count"] = len(conn.ConnectionSet)
		result["messageModel"] = conn.MessageModel
		result["consumeType"] = conn.ConsumeType
		if len(conn.ConnectionSet) > 0 {
			version := conn.ConnectionSet[0].Version
			for _, client := range conn.ConnectionSet {
				if client.Version < version {
					version = client.Version
				}
			}
			result["version"] = fmt.Sprintf("UNKNOWN(%d)", version)
			if version >= 0 && version < len(dashboardVersionNames) {
				result["version"] = dashboardVersionNames[version]
			}
		}
	}
	return result, nil
}

func (s *session) dashboardConsumeStats(ctx context.Context, c *admin.Client, group, topic string) ([]map[string]any, error) {
	stats, err := s.dashboardRawStats(ctx, c, group, topic)
	if err != nil {
		return nil, err
	}
	byTopic := map[string]map[string]any{}
	for key, o := range stats.OffsetTable {
		q := parseMessageQueueKey(key)
		if topic != "" && q.Topic != topic {
			continue
		}
		row := byTopic[q.Topic]
		if row == nil {
			row = map[string]any{"topic": q.Topic, "diffTotal": int64(0), "lastTimestamp": int64(0), "queueStatInfoList": []any{}}
			byTopic[q.Topic] = row
		}
		row["diffTotal"] = row["diffTotal"].(int64) + o.BrokerOffset - o.ConsumerOffset
		row["lastTimestamp"] = maxValue(row["lastTimestamp"].(int64), o.LastTimestamp)
		row["queueStatInfoList"] = append(row["queueStatInfoList"].([]any), map[string]any{"brokerName": q.BrokerName, "queueId": q.QueueID, "brokerOffset": o.BrokerOffset, "consumerOffset": o.ConsumerOffset, "diffTotal": o.BrokerOffset - o.ConsumerOffset, "lastTimestamp": o.LastTimestamp, "clientInfo": ""})
	}
	rows := []map[string]any{}
	for _, name := range sortedKeys(byTopic) {
		clients := resolveConsumerClients(ctx, remotingConsumeStatusReader{client: c, invoke: invokeRemotingWithClient}, group, name)
		for _, raw := range byTopic[name]["queueStatInfoList"].([]any) {
			q := raw.(map[string]any)
			q["clientInfo"] = clients[parsedMessageQueue{Topic: name, BrokerName: q["brokerName"].(string), QueueID: q["queueId"].(int)}]
		}
		rows = append(rows, byTopic[name])
	}
	sort.Slice(rows, func(i, j int) bool {
		return strings.Compare(fmt.Sprint(rows[i]["topic"]), fmt.Sprint(rows[j]["topic"])) < 0
	})
	return rows, nil
}
