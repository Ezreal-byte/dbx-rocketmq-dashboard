package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	admin "github.com/amigoer/rocketmq-admin-go"
	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
)

func rocketMQCode(err error, code int) bool {
	var e *admin.AdminError
	return errors.As(err, &e) && e.Code == code
}

func (s *session) dashboardProducer(ctx context.Context, c *admin.Client, group, topic string) (*admin.ProducerConnection, error) {
	if group == "" || topic == "" {
		return nil, fmt.Errorf("producerGroup and topic are required")
	}
	route, err := c.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		return nil, err
	}
	result := &admin.ProducerConnection{}
	seen := map[string]bool{}
	for _, addr := range masterAddressesFromRoute(route) {
		r, e := invokeRemotingAllowCodes(ctx, addr, s.connection.ConnectTimeout, remoting.NewRequest(remoting.GetProducerConnectionList, map[string]string{"producerGroup": group, "topic": topic}), 0, 204)
		if e != nil {
			return nil, e
		}
		if r.Code == 204 {
			continue
		}
		var local admin.ProducerConnection
		if e = json.Unmarshal(r.Body, &local); e != nil {
			return nil, e
		}
		for _, conn := range local.ConnectionSet {
			key := conn.ClientId + "@" + conn.ClientAddr
			if !seen[key] {
				seen[key] = true
				result.ConnectionSet = append(result.ConnectionSet, conn)
			}
		}
	}
	if len(result.ConnectionSet) == 0 {
		return nil, fmt.Errorf("producer group is not online: %s", group)
	}
	return result, nil
}

func (s *session) dashboardTopicGroups(ctx context.Context, c *admin.Client, topic string) ([]string, error) {
	route, err := c.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		return nil, err
	}
	groups := map[string]bool{}
	for _, addr := range masterAddressesFromRoute(route) {
		r, e := invokeRemotingWithClient(ctx, addr, remoting.NewRequest(remoting.QueryTopicConsumeByWho, map[string]string{"topic": topic}))
		if e != nil {
			return nil, e
		}
		var data struct {
			GroupList []string `json:"groupList"`
		}
		if e = json.Unmarshal(r.Body, &data); e != nil {
			return nil, e
		}
		for _, g := range data.GroupList {
			groups[g] = true
		}
	}
	return sortedKeys(groups), nil
}

func (s *session) dashboardClientControl(ctx context.Context, c *admin.Client, group, clientID string, code int, extra map[string]string) (any, error) {
	conn, err := s.dashboardConnection(ctx, c, group, "")
	if err != nil {
		return nil, err
	}
	found := false
	for _, v := range conn.ConnectionSet {
		found = found || v.ClientId == clientID
	}
	if !found {
		return nil, fmt.Errorf("client is not connected: %s", clientID)
	}
	brokers, err := s.dashboardBrokers(ctx, c, map[string]any{})
	if err != nil {
		return nil, err
	}
	extra["consumerGroup"], extra["clientId"] = group, clientID
	for _, name := range sortedMapKeys(brokers) {
		r, e := invokeRemotingAllowCodes(ctx, brokers[name], s.connection.ConnectTimeout, remoting.NewRequest(remoting.GetConsumerConnectionList, map[string]string{"consumerGroup": group}), 0, 206)
		if e != nil {
			return nil, e
		}
		if r.Code == 206 {
			continue
		}
		var local admin.ConsumerConnection
		if e = json.Unmarshal(r.Body, &local); e != nil {
			return nil, e
		}
		for _, v := range local.ConnectionSet {
			if v.ClientId == clientID {
				r, e = invokeRemotingWithClient(ctx, brokers[name], remoting.NewRequest(code, extra))
				if e != nil {
					return nil, e
				}
				var data any
				e = json.Unmarshal(repairRunningInfo(r.Body), &data)
				return data, e
			}
		}
	}
	return nil, fmt.Errorf("client disconnected during request")
}

func repairRunningInfo(body []byte) []byte {
	b := repairRocketMQJSON(body)
	if json.Valid(b) {
		return b
	}
	// rocketmq-client-go 2.1.2 appends JStack without JSON escaping. It is
	// the final field; preserve the stack verbatim while repairing that field.
	marker := []byte(`"jstack":"`)
	i := bytes.LastIndex(b, marker)
	end := bytes.LastIndexByte(b, '"')
	if i >= 0 && end >= i+len(marker) {
		encoded, _ := json.Marshal(string(b[i+len(marker) : end]))
		fixed := append([]byte{}, b[:i+len(marker)-1]...)
		fixed = append(fixed, encoded...)
		fixed = append(fixed, b[end+1:]...)
		return fixed
	}
	return b
}

// Offline is distinct from permission, network and decoding errors.
func (s *session) dashboardConnection(ctx context.Context, c *admin.Client, group, proxyAddr string) (*admin.ConsumerConnection, error) {
	brokers, err := s.dashboardBrokers(ctx, c, map[string]any{})
	if err != nil {
		return nil, err
	}
	if proxyAddr == "undefined" || proxyAddr == "null" {
		proxyAddr = ""
	}
	if proxyAddr != "" {
		selected := map[string]string{}
		for _, wanted := range splitAddresses(proxyAddr) {
			for name, addr := range brokers {
				if wanted == addr || wanted == s.proxies.OriginalForLocal(addr) {
					selected[name] = addr
				}
			}
		}
		if len(selected) == len(splitAddresses(proxyAddr)) {
			brokers = selected
			proxyAddr = ""
		}
	}
	if proxyAddr != "" {
		allowed := false
		for _, addr := range splitAddresses(s.connection.ProxyAddr) {
			allowed = allowed || addr == proxyAddr
		}
		if !allowed {
			return nil, fmt.Errorf("Proxy address is not configured in this DBX connection")
		}
		addr, e := s.proxies.ProxyFor(proxyAddr, proxyTargetProxy)
		if e != nil {
			return nil, e
		}
		brokers = map[string]string{"proxy": addr}
	}
	result := &admin.ConsumerConnection{ConnectionSet: []*admin.Connection{}, SubscriptionTable: map[string]*admin.SubscriptionData{}}
	seen := map[string]bool{}
	for _, name := range sortedMapKeys(brokers) {
		response, e := invokeRemotingAllowCodes(ctx, brokers[name], s.connection.ConnectTimeout, remoting.NewRequest(remoting.GetConsumerConnectionList, map[string]string{"consumerGroup": group}), 0, 206)
		if e != nil {
			return nil, fmt.Errorf("%s: %w", name, e)
		}
		if response.Code == 206 {
			continue
		}
		var conn admin.ConsumerConnection
		if e = json.Unmarshal(response.Body, &conn); e != nil {
			return nil, e
		}
		result.ConsumeType, result.MessageModel, result.ConsumeFromWhere = conn.ConsumeType, conn.MessageModel, conn.ConsumeFromWhere
		for k, v := range conn.SubscriptionTable {
			result.SubscriptionTable[k] = v
		}
		for _, entry := range conn.ConnectionSet {
			if !seen[entry.ClientId] {
				seen[entry.ClientId] = true
				result.ConnectionSet = append(result.ConnectionSet, entry)
			}
		}
	}
	if len(result.ConnectionSet) == 0 {
		return nil, admin.NewAdminError(206, "consumer group is offline: "+group)
	}
	return result, nil
}

func (s *session) dashboardRawStats(ctx context.Context, c *admin.Client, group, topic string) (*admin.ConsumeStats, error) {
	brokers, err := s.dashboardBrokers(ctx, c, map[string]any{})
	if err != nil {
		return nil, err
	}
	merged := &admin.ConsumeStats{OffsetTable: map[string]*admin.OffsetWrapper{}}
	for _, name := range sortedMapKeys(brokers) {
		response, e := invokeRemotingAllowCodes(ctx, brokers[name], s.connection.ConnectTimeout, remoting.NewRequest(remoting.GetConsumeStats, map[string]string{"consumerGroup": group, "topic": topic}), 0, 203, 207)
		if e != nil {
			return nil, fmt.Errorf("%s: %w", name, e)
		}
		// SUBSCRIPTION_GROUP_NOT_EXIST / CONSUME_STATS_NOT_EXIST on a broker
		// that has never held offsets is an empty dataset, not a transport failure.
		if response.Code != 0 {
			continue
		}
		part, e := decodeConsumeStats(response.Body)
		if e != nil {
			return nil, e
		}
		for k, v := range part.OffsetTable {
			merged.OffsetTable[k] = v
		}
		merged.ConsumeTps += part.ConsumeTps
	}
	return merged, nil
}

func (s *session) dashboardReset(ctx context.Context, c *admin.Client, topic, group string, timestamp int64, force bool) ([]any, error) {
	route, err := c.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		return nil, err
	}
	rows := []any{}
	brokers := map[string]string{}
	for _, b := range route.BrokerDatas {
		if b.BrokerAddrs["0"] != "" {
			brokers[b.BrokerName] = b.BrokerAddrs["0"]
		}
	}
	_, err = mutateBrokers(brokers, func(addr string) error {
		response, e := invokeRemotingAllowCodes(ctx, addr, s.connection.ConnectTimeout, remoting.NewRequest(222, map[string]string{"topic": topic, "group": group, "timestamp": strconv.FormatInt(timestamp, 10), "isForce": strconv.FormatBool(force)}), 0, 206)
		if e != nil {
			return e
		}
		if response.Code == 0 {
			var offsets struct {
				OffsetTable map[string]int64 `json:"offsetTable"`
			}
			if e = json.Unmarshal(repairConsumerStatusJSON(response.Body), &offsets); e != nil {
				return e
			}
			for key, offset := range offsets.OffsetTable {
				q := parseMessageQueueKey(key)
				rows = append(rows, map[string]any{"brokerName": q.BrokerName, "queueId": q.QueueID, "rollbackOffset": offset})
			}
			return nil
		}
		// Match Dashboard's offline fallback, but report every failed write.
		response, e = invokeRemotingWithClient(ctx, addr, remoting.NewRequest(remoting.GetConsumeStats, map[string]string{"consumerGroup": group, "topic": topic}))
		if e != nil {
			return e
		}
		stats, e := decodeConsumeStats(response.Body)
		if e != nil {
			return e
		}
		for key, old := range stats.OffsetTable {
			q := parseMessageQueueKey(key)
			if q.Topic != topic {
				continue
			}
			var offset int64
			if timestamp < 0 {
				offset, e = queueOffset(ctx, addr, topic, q.QueueID, remoting.GetMaxOffset)
			} else {
				offset, e = c.SearchOffset(ctx, addr, topic, q.QueueID, timestamp)
			}
			if e != nil {
				return e
			}
			if e = c.UpdateConsumeOffset(ctx, addr, group, topic, q.QueueID, offset); e != nil {
				return e
			}
			rows = append(rows, map[string]any{"brokerName": q.BrokerName, "queueId": q.QueueID, "rollbackOffset": offset, "consumerOffset": old.ConsumerOffset, "brokerOffset": old.BrokerOffset})
		}
		return nil
	})
	return rows, err
}

func (s *session) dashboardTracks(ctx context.Context, c *admin.Client, msg *admin.MessageExt) ([]admin.MessageTrack, error) {
	groups, err := s.dashboardTopicGroups(ctx, c, msg.Topic)
	if err != nil {
		return nil, err
	}
	tracks := []admin.MessageTrack{}
	for _, group := range groups {
		track := admin.MessageTrack{ConsumerGroup: group, TrackType: "UNKNOWN"}
		conn, e := s.dashboardConnection(ctx, c, group, "")
		if e != nil {
			if rocketMQCode(e, 206) {
				track.TrackType = "NOT_ONLINE"
			}
			track.ExceptionDesc = e.Error()
		} else if conn.ConsumeType == "CONSUME_ACTIVELY" {
			track.TrackType = "PULL"
		} else {
			stats, e := s.dashboardRawStats(ctx, c, group, msg.Topic)
			if e != nil {
				track.ExceptionDesc = e.Error()
			} else {
				track.TrackType = "NOT_CONSUME_YET"
				for key, o := range stats.OffsetTable {
					q := parseMessageQueueKey(key)
					if q.Topic == msg.Topic && q.BrokerName == msg.BrokerName && q.QueueID == msg.QueueId && o.ConsumerOffset > msg.QueueOffset {
						track.TrackType = "CONSUMED"
						if sub := conn.SubscriptionTable[msg.Topic]; sub != nil && (sub.ExpressionType == "TAG" || sub.ExpressionType == "") && sub.SubString != "*" {
							matched := false
							for _, tag := range strings.Split(sub.SubString, "||") {
								matched = matched || strings.TrimSpace(tag) == msg.Properties["TAGS"]
							}
							if !matched {
								track.TrackType = "CONSUMED_BUT_FILTERED"
							}
						}
					}
				}
			}
		}
		tracks = append(tracks, track)
	}
	return tracks, nil
}
