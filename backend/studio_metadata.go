package main

import (
	"context"
	"fmt"
	admin "github.com/amigoer/rocketmq-admin-go"
	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
	"strings"
	"time"
)

func studioPerm(perm int) string {
	switch perm & 6 {
	case 6:
		return "RW"
	case 4:
		return "RO"
	case 2:
		return "WO"
	default:
		return "DENY"
	}
}
func (s *session) studioTopic(ctx context.Context, c *admin.Client, name string, cfg *topicConfigWire) (map[string]any, error) {
	stats, e := s.examineTopicStats(ctx, c, name)
	if e != nil {
		return nil, e
	}
	var count int64
	for _, v := range stats {
		count += maxValue(int64(0), v.MaxOffset-v.MinOffset)
	}
	groups, e := s.dashboardTopicGroups(ctx, c, name)
	if e != nil {
		return nil, e
	}
	return map[string]any{"name": name, "namespace": "", "instanceId": "1", "clusterId": s.clusterName, "type": valueOrDefault(cfg.Attributes["message.type"], "NORMAL"), "writeQueues": cfg.WriteQueueNums, "readQueues": cfg.ReadQueueNums, "perm": studioPerm(cfg.Perm), "messageCount": count, "tps": nil, "consumerGroupCount": len(groups), "remark": "", "gmtCreate": nil, "gmtModified": nil}, nil
}
func (s *session) studioGroup(ctx context.Context, c *admin.Client, name string, cfg *subscriptionGroupConfig) (map[string]any, error) {
	conn, e := s.dashboardConnection(ctx, c, name, "")
	if e != nil && !rocketMQCode(e, 206) {
		return nil, e
	}
	stats, e := s.dashboardRawStats(ctx, c, name, "")
	if e != nil {
		return nil, e
	}
	topics := map[string]bool{}
	lagByTopic := map[string]int64{}
	var lag int64
	var oldest int64
	for key, v := range stats.OffsetTable {
		q := parseMessageQueueKey(key)
		topics[q.Topic] = true
		diff := maxValue(int64(0), v.BrokerOffset-v.ConsumerOffset)
		lag += diff
		lagByTopic[q.Topic] += diff
		if diff > 0 && v.LastTimestamp > 0 && (oldest == 0 || v.LastTimestamp < oldest) {
			oldest = v.LastTimestamp
		}
	}
	instances := []any{}
	consumeType := ""
	mode := ""
	filterType := ""
	if conn != nil {
		consumeType = conn.ConsumeType
		switch consumeType {
		case "CONSUME_PASSIVELY":
			mode = "Push"
		case "CONSUME_ACTIVELY":
			mode = "Pull"
		case "CONSUME_POP":
			mode = "Pop"
		}
		consumeType = conn.MessageModel
		for t, sub := range conn.SubscriptionTable {
			topics[t] = true
			if filterType == "" {
				filterType = valueOrDefault(sub.ExpressionType, "TAG")
			}
		}
		for _, v := range conn.ConnectionSet {
			instances = append(instances, map[string]any{"clientId": v.ClientId, "protocol": "Remoting", "address": v.ClientAddr, "subscribedTopics": sortedKeys(conn.SubscriptionTable), "lastHeartbeat": nil, "topicLag": lagByTopic})
		}
	}
	var delay any
	if lag == 0 {
		delay = 0
	} else if oldest > 0 {
		delay = maxValue(int64(0), (time.Now().UnixMilli()-oldest)/1000)
	}
	return map[string]any{"name": name, "namespace": "", "clusterId": s.clusterName, "instanceId": "1", "subscriptionMode": mode, "consumeType": consumeType, "onlineInstances": len(instances), "totalLag": lag, "subscribedTopics": sortedKeys(topics), "subscriptionDataType": filterType, "deliveryOrderType": map[bool]string{true: "FIFO", false: "NORMAL"}[cfg.ConsumeMessageOrderly], "retryMaxTimes": cfg.RetryMaxTimes, "gmtCreate": nil, "gmtModified": nil, "delaySeconds": delay, "instances": instances}, nil
}
func (s *session) studioMetadata(ctx context.Context, c *admin.Client, path string, p map[string]any) (any, error) {
	topicMode := strings.HasPrefix(path, "/topics")
	name := stringValue(p, "id", "name")
	base := "/groups"
	if topicMode {
		base = "/topics"
	}
	if path == base+"/import" {
		key := "groups"
		if topicMode {
			key = "topics"
		}
		entries, ok := p[key].([]any)
		if !ok || len(entries) == 0 || len(entries) > 1000 {
			return nil, fmt.Errorf("import requires 1–1000 %s", key)
		}
		rows := []any{}
		failed := []any{}
		for i, raw := range entries {
			entry, ok := raw.(map[string]any)
			if !ok {
				failed = append(failed, map[string]any{"index": i, "message": "invalid entry"})
				continue
			}
			value, e := s.studioMetadata(ctx, c, base+"/create", entry)
			if e != nil {
				failed = append(failed, map[string]any{"index": i, "name": entry["name"], "message": e.Error()})
			} else {
				rows = append(rows, value)
			}
		}
		return map[string]any{"imported": len(rows), "failed": len(failed), key: rows, "failures": failed}, nil
	}
	if path == base || path == base+"/page" || path == base+"/export" {
		rows := []any{}
		if topicMode {
			configs, complete := s.collectTopicConfigs(ctx)
			if !complete {
				return nil, fmt.Errorf("topic inventory incomplete")
			}
			for _, n := range sortedKeys(configs) {
				cfg := configs[n]
				kind := classifyTopicMessageType(n, s.clusterName, cfg.Attributes)
				if kind == "SYSTEM" || kind == "RETRY" || kind == "DLQ" || !studioMatches(n, p) {
					continue
				}
				if t := stringValue(p, "type"); t != "" && !strings.EqualFold(t, kind) {
					continue
				}
				row, e := s.studioTopic(ctx, c, n, cfg)
				if e != nil {
					return nil, e
				}
				rows = append(rows, row)
			}
		} else {
			configs, e := s.collectSubscriptionGroupConfigs(ctx)
			if e != nil {
				return nil, e
			}
			for _, n := range sortedKeys(configs) {
				cfg := configs[n]
				if classifyConsumerGroup(n, cfg) == "SYSTEM" {
					continue
				}
				row, e := s.studioGroup(ctx, c, n, cfg)
				if e != nil {
					return nil, e
				}
				if !studioMatches(n+" "+strings.Join(stringSlice(row["subscribedTopics"]), " "), p) && !studioMatches(n, p) {
					continue
				}
				if mode := stringValue(p, "subscriptionMode"); mode != "" && !strings.EqualFold(mode, stringValue(row, "subscriptionMode")) {
					continue
				}
				rows = append(rows, row)
			}
		}
		if strings.HasSuffix(path, "/page") {
			return studioPage(rows, p), nil
		}
		if strings.HasSuffix(path, "/export") {
			return studioResourceCSV(rows, topicMode)
		}
		return rows, nil
	}
	if path == "/topics/send" {
		q := map[string]any{"topic": p["topic"], "messageBody": p["body"], "tag": p["tag"], "key": p["key"], "properties": p["properties"]}
		v, e := s.dashboardSend(ctx, c, q)
		if e != nil {
			return v, e
		}
		row := jsonObject(v)
		row["sendTime"] = time.Now().UnixMilli()
		return row, nil
	}
	if path == base+"/delete" {
		q := map[string]any{"topic": name, "groupName": name}
		if topicMode {
			return s.dashboardTopic(ctx, c, "/topic/deleteTopic.do", q)
		}
		return s.dashboardConsumer(ctx, c, "/consumer/deleteSubGroup.do", q)
	}
	if path == base+"/create" || path == base+"/update" || path == "/groups/settings" {
		if name == "" {
			return nil, fmt.Errorf("name is required")
		}
		brokers, e := s.dashboardBrokers(ctx, c, map[string]any{})
		if e != nil {
			return nil, e
		}
		names := []any{}
		for _, n := range sortedMapKeys(brokers) {
			names = append(names, n)
		}
		if topicMode {
			perm := 6
			switch strings.ToUpper(stringValue(p, "perm")) {
			case "R", "RO":
				perm = 4
			case "W", "WO":
				perm = 2
			case "DENY":
				perm = 0
			}
			q := map[string]any{"topicName": name, "readQueueNums": float64(intValue(p, 8, "readQueues")), "writeQueueNums": float64(intValue(p, 8, "writeQueues")), "perm": float64(perm), "messageType": valueOrDefault(stringValue(p, "type"), "NORMAL"), "brokerNameList": names}
			if _, e = s.dashboardTopic(ctx, c, "/topic/createOrUpdate.do", q); e != nil {
				return nil, e
			}
			cfg, e := readTopicConfig(ctx, brokers[sortedMapKeys(brokers)[0]], name)
			if e != nil {
				return nil, e
			}
			return s.studioTopic(ctx, c, name, cfg)
		}
		cfg := &subscriptionGroupConfig{GroupName: name, ConsumeEnable: true, ConsumeBroadcastEnable: true, RetryQueueNums: 1, RetryMaxTimes: 16, NotifyConsumerIDsChangedEnable: true}
		if path == "/groups/settings" {
			configs, e := s.collectSubscriptionGroupConfigs(ctx)
			if e != nil {
				return nil, e
			}
			if configs[name] == nil {
				return nil, fmt.Errorf("group not found")
			}
			cfg = configs[name]
		}
		cfg.RetryQueueNums = intValue(p, cfg.RetryQueueNums, "retryQueueNums")
		cfg.RetryMaxTimes = intValue(p, cfg.RetryMaxTimes, "retryMaxTimes")
		cfg.ConsumeEnable = boolValue(p, cfg.ConsumeEnable, "consumeEnable")
		cfg.ConsumeBroadcastEnable = boolValue(p, cfg.ConsumeBroadcastEnable, "consumeBroadcastEnable")
		cfg.ConsumeMessageOrderly = boolValue(p, cfg.ConsumeMessageOrderly, "consumeMessageOrderly")
		if stringValue(p, "deliveryOrderType") == "FIFO" {
			cfg.ConsumeMessageOrderly = true
		}
		if _, e = mutateBrokers(brokers, func(addr string) error { return writeSubscriptionGroupConfig(ctx, addr, cfg) }); e != nil {
			return nil, e
		}
		if path == "/groups/settings" {
			return cfg, nil
		}
		return s.studioGroup(ctx, c, name, cfg)
	}
	if path == "/topics/:id/routes" {
		route, e := c.ExamineTopicRouteInfo(ctx, name)
		if e != nil {
			return nil, e
		}
		rows := []any{}
		for _, q := range route.QueueDatas {
			for _, b := range route.BrokerDatas {
				if b.BrokerName != q.BrokerName {
					continue
				}
				addrs := map[string]string{}
				for id, a := range b.BrokerAddrs {
					addrs[id] = valueOrDefault(s.proxies.OriginalForLocal(a), a)
				}
				rows = append(rows, map[string]any{"brokerName": b.BrokerName, "brokerAddr": addrs["0"], "masterAddr": addrs["0"], "brokerAddrs": addrs, "replicaCount": len(addrs), "writeQueues": q.WriteQueueNums, "readQueues": q.ReadQueueNums, "perm": studioPerm(q.Perm), "permCode": q.Perm, "readable": q.Perm&4 != 0, "writable": q.Perm&2 != 0, "topicSysFlag": q.TopicSysFlag})
			}
		}
		return rows, nil
	}
	if strings.HasPrefix(path, "/topics/:id/consumers") {
		groups, e := s.dashboardTopicGroups(ctx, c, name)
		if e != nil {
			return nil, e
		}
		rows := []any{}
		for _, g := range groups {
			row, e := s.dashboardGroup(ctx, c, g)
			if e != nil {
				return nil, e
			}
			row["metricsAvailable"] = true
			rows = append(rows, row)
		}
		if strings.HasSuffix(path, "/page") {
			return studioPage(rows, p), nil
		}
		return rows, nil
	}
	if strings.HasPrefix(path, "/groups/reset-offset") {
		return s.studioReset(ctx, c, path, p)
	}
	configs, e := s.collectSubscriptionGroupConfigs(ctx)
	if e != nil {
		return nil, e
	}
	cfg := configs[name]
	if cfg == nil {
		return nil, fmt.Errorf("consumer group not found: %s", name)
	}
	switch path {
	case "/groups/:id", "/groups/:id/refresh":
		return s.studioGroup(ctx, c, name, cfg)
	case "/groups/:id/settings":
		return cfg, nil
	case "/groups/:id/progress":
		stats, e := s.dashboardRawStats(ctx, c, name, "")
		if e != nil {
			return nil, e
		}
		rows := []any{}
		for _, key := range sortedKeys(stats.OffsetTable) {
			q := parseMessageQueueKey(key)
			v := stats.OffsetTable[key]
			rows = append(rows, map[string]any{"topic": q.Topic, "broker": q.BrokerName, "queueId": q.QueueID, "brokerOffset": v.BrokerOffset, "consumerOffset": v.ConsumerOffset, "diffTotal": v.BrokerOffset - v.ConsumerOffset})
		}
		return rows, nil
	case "/groups/:id/subscriptions":
		conn, e := s.dashboardConnection(ctx, c, name, "")
		if rocketMQCode(e, 206) {
			return []any{}, nil
		}
		if e != nil {
			return nil, e
		}
		rows := []any{}
		for _, topic := range sortedKeys(conn.SubscriptionTable) {
			sub := conn.SubscriptionTable[topic]
			rows = append(rows, map[string]any{"topic": topic, "expression": sub.SubString, "type": valueOrDefault(sub.ExpressionType, "TAG"), "filterMode": valueOrDefault(sub.ExpressionType, "TAG"), "consistency": "unknown"})
		}
		return rows, nil
	case "/groups/:id/instances/:client/stack":
		v, e := s.dashboardClientControl(ctx, c, name, stringValue(p, "client"), remoting.GetConsumerRunningInfo, map[string]string{"jstackEnable": "true"})
		if e != nil {
			return nil, e
		}
		stack := stringValue(jsonObject(v), "jstack")
		if stack == "" {
			return nil, fmt.Errorf("client returned no thread stack")
		}
		return map[string]any{"groupName": name, "clientId": p["client"], "capturedAt": time.Now().Format(time.RFC3339), "threadCount": 1, "threads": []any{map[string]any{"threadName": "Client stack dump", "threadId": 0, "state": "", "blockedTime": -1, "waitedTime": -1, "stackTrace": strings.Split(stack, "\n")}}}, nil
	}
	return nil, fmt.Errorf("unhandled Studio metadata endpoint: %s", path)
}

func (s *session) studioReset(ctx context.Context, c *admin.Client, path string, p map[string]any) (any, error) {
	group, topic := stringValue(p, "name"), stringValue(p, "topic")
	timestamp := int64Value(p, -1, "timestamp")
	if group == "" || topic == "" || timestamp < 0 {
		return nil, fmt.Errorf("group, topic and timestamp required")
	}
	stats, e := s.dashboardRawStats(ctx, c, group, topic)
	if e != nil {
		return nil, e
	}
	targets, e := s.messageQueueTargets(ctx, c, topic, false)
	if e != nil {
		return nil, e
	}
	rows := []any{}
	var current, projected, delta int64
	rewind, forward := 0, 0
	for _, target := range targets {
		min, e := queueOffset(ctx, target.Address, topic, target.QueueID, remoting.GetMinOffset)
		if e != nil {
			return nil, e
		}
		max, e := queueOffset(ctx, target.Address, topic, target.QueueID, remoting.GetMaxOffset)
		if e != nil {
			return nil, e
		}
		offset, e := c.SearchOffset(ctx, target.Address, topic, target.QueueID, timestamp)
		if e != nil {
			return nil, e
		}
		offset = maxValue(min, minValue(max, offset))
		consumer := min
		for k, v := range stats.OffsetTable {
			q := parseMessageQueueKey(k)
			if q.Topic == topic && q.BrokerName == target.BrokerName && q.QueueID == target.QueueID {
				consumer = v.ConsumerOffset
			}
		}
		change := offset - consumer
		risk := "NONE"
		if change < 0 {
			rewind++
			risk = "REWIND"
		}
		if change > 0 {
			forward++
			risk = "FAST_FORWARD"
		}
		current += maxValue(int64(0), max-consumer)
		projected += max - offset
		delta += change
		rows = append(rows, map[string]any{"topic": topic, "broker": target.BrokerName, "queueId": target.QueueID, "minOffset": min, "maxOffset": max, "brokerOffset": max, "consumerOffset": consumer, "targetOffset": offset, "currentLag": maxValue(int64(0), max-consumer), "projectedLag": max - offset, "offsetDelta": change, "riskLevel": risk, "message": ""})
	}
	if path == "/groups/reset-offset" {
		return s.dashboardReset(ctx, c, topic, group, timestamp, true)
	}
	return map[string]any{"instanceId": "1", "groupName": group, "topic": topic, "timestamp": timestamp, "complete": true, "allowReset": len(rows) > 0, "queueCount": len(rows), "warningCount": 0, "rewindQueueCount": rewind, "fastForwardQueueCount": forward, "currentTotalLag": current, "projectedTotalLag": projected, "totalOffsetDelta": delta, "warnings": []any{}, "queues": rows}, nil
}
