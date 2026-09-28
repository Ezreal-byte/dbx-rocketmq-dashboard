package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	admin "github.com/amigoer/rocketmq-admin-go"
	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
)

func (s *session) dashboardTopic(ctx context.Context, c *admin.Client, path string, p map[string]any) (any, error) {
	topic := stringValue(p, "topic", "topicName")
	switch path {
	case "/topic/list.query", "/topic/list.queryTopicType":
		topics, err := c.FetchAllTopicList(ctx)
		if err != nil {
			return nil, err
		}
		configs, complete := s.collectTopicConfigs(ctx)
		if !complete {
			return nil, fmt.Errorf("could not read topic configurations from all brokers")
		}
		names := []string{}
		types := []string{}
		for _, name := range topics.TopicList {
			if boolValue(p, false, "skipRetryAndDlq") && (strings.HasPrefix(name, "%RETRY%") || strings.HasPrefix(name, "%DLQ%")) {
				continue
			}
			var attrs map[string]string
			if cfg := configs[name]; cfg != nil {
				attrs = cfg.Attributes
			}
			kind := classifyTopicMessageType(name, s.clusterName, attrs)
			if kind == "SYSTEM" && (path == "/topic/list.queryTopicType" || !boolValue(p, false, "skipSysProcess")) {
				name = "%SYS%" + name
			}
			names = append(names, name)
			types = append(types, kind)
		}
		if path == "/topic/list.queryTopicType" {
			return map[string]any{"topicNameList": names, "messageTypeList": types}, nil
		}
		return map[string]any{"topicList": names}, nil
	case "/topic/route.query":
		return s.getTopicRoute(map[string]any{"topic": topic})
	case "/topic/stats.query":
		stats, err := s.examineTopicStats(ctx, c, topic)
		if err != nil {
			return nil, err
		}
		return map[string]any{"offsetTable": stats}, nil
	case "/topic/queryTopicConsumerInfo.query":
		groups, err := s.dashboardTopicGroups(ctx, c, topic)
		if groups == nil {
			groups = []string{}
		}
		return map[string]any{"groupList": groups}, err
	case "/topic/queryConsumerByTopic.query":
		groups, err := s.dashboardTopicGroups(ctx, c, topic)
		if err != nil {
			return nil, err
		}
		result := map[string]any{}
		for _, group := range groups {
			rows, e := s.dashboardConsumeStats(ctx, c, group, topic)
			if e != nil {
				return nil, e
			}
			if len(rows) > 0 {
				result[group] = rows[0]
			}
		}
		return result, nil
	case "/topic/examineTopicConfig.query":
		route, err := c.ExamineTopicRouteInfo(ctx, topic)
		if err != nil {
			return nil, err
		}
		rows := []any{}
		for _, b := range route.BrokerDatas {
			cfg, e := readTopicConfig(ctx, b.BrokerAddrs["0"], topic)
			if e != nil {
				return nil, e
			}
			row := jsonObject(cfg)
			row["brokerNameList"] = []string{b.BrokerName}
			row["messageType"] = valueOrDefault(cfg.Attributes["message.type"], "UNSPECIFIED")
			rows = append(rows, row)
		}
		return rows, nil
	case "/topic/createOrUpdate.do":
		if topic == "" {
			return nil, fmt.Errorf("topicName is required")
		}
		if len(stringSlice(p["brokerNameList"]))+len(stringSlice(p["clusterNameList"])) == 0 {
			return nil, fmt.Errorf("select at least one cluster or broker")
		}
		brokers, err := s.dashboardBrokers(ctx, c, p)
		if err != nil {
			return nil, err
		}
		raw, _ := json.Marshal(p)
		cfg := topicConfigWire{ReadQueueNums: 8, WriteQueueNums: 8, Perm: 6, TopicFilterType: "SINGLE_TAG"}
		if err = json.Unmarshal(raw, &cfg); err != nil {
			return nil, err
		}
		cfg.TopicName = topic
		if cfg.ReadQueueNums < 1 || cfg.WriteQueueNums < 1 {
			return nil, fmt.Errorf("queue counts must be positive")
		}
		cfg.Attributes = map[string]string{"+message.type": valueOrDefault(stringValue(p, "messageType"), "NORMAL")}
		return mutateBrokers(brokers, func(addr string) error { return writeTopicConfig(ctx, addr, &cfg) })
	case "/topic/deleteTopic.do", "/topic/deleteTopicByBroker.do":
		if topic == "" {
			return nil, fmt.Errorf("topic is required")
		}
		if path == "/topic/deleteTopicByBroker.do" && stringValue(p, "brokerName") == "" {
			return nil, fmt.Errorf("brokerName is required")
		}
		brokers, err := s.dashboardBrokers(ctx, c, p)
		if err != nil {
			return nil, err
		}
		_, err = mutateBrokers(brokers, func(addr string) error { return c.DeleteTopicInBroker(ctx, addr, topic) })
		if err != nil {
			return nil, err
		}
		if path == "/topic/deleteTopic.do" {
			if err = s.dashboardDeleteTopicNS(ctx, c, topic); err != nil {
				return nil, fmt.Errorf("brokers deleted topic, NameServer cleanup failed: %w", err)
			}
		}
		return true, nil
	case "/topic/sendTopicMessage.do":
		return s.dashboardSend(ctx, c, p)
	}
	return nil, fmt.Errorf("unknown topic operation %s", path)
}

func (s *session) dashboardDeleteTopicNS(ctx context.Context, c *admin.Client, topic string) error {
	servers := map[string]string{}
	for _, addr := range c.GetNameServerAddressList() {
		servers[s.proxies.OriginalForLocal(addr)] = addr
	}
	_, err := mutateBrokers(servers, func(addr string) error {
		_, e := invokeRemotingWithClient(ctx, addr, remoting.NewRequest(remoting.DeleteTopicInNamesrv, map[string]string{"topic": topic}))
		return e
	})
	return err
}
