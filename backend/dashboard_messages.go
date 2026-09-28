package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	admin "github.com/amigoer/rocketmq-admin-go"
	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
	rocketmq "github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

func messageView(m *admin.MessageExt) map[string]any {
	row := jsonObject(m)
	delete(row, "body")
	row["messageBody"] = string(m.Body)
	return row
}
func messageViews(messages []*admin.MessageExt) []any {
	rows := []any{}
	for _, m := range messages {
		rows = append(rows, messageView(m))
	}
	return rows
}

func (s *session) dashboardMessage(ctx context.Context, c *admin.Client, path string, p map[string]any) (any, error) {
	topic := stringValue(p, "topic", "topicName")
	id := stringValue(p, "msgId")
	switch path {
	case "/message/viewMessage.query", "/messageTrace/viewMessage.query", "/dlqMessage/exportDlqMessage.do":
		if topic == "" && stringValue(p, "consumerGroup") != "" {
			topic = "%DLQ%" + stringValue(p, "consumerGroup")
		}
		msg, err := s.dashboardView(ctx, c, topic, id)
		if err != nil {
			return nil, err
		}
		if path == "/dlqMessage/exportDlqMessage.do" {
			return messageView(msg), nil
		}
		if path == "/messageTrace/viewMessage.query" {
			return map[string]any{"messageView": messageView(msg)}, nil
		}
		tracks, err := s.dashboardTracks(ctx, c, msg)
		if err != nil {
			return nil, err
		}
		if tracks == nil {
			tracks = []admin.MessageTrack{}
		}
		return map[string]any{"messageView": messageView(msg), "messageTrackList": tracks}, nil
	case "/message/queryMessageByTopicAndKey.query":
		rows, err := queryMessagesByKey(ctx, c, s.connection.ConnectTimeout, topic, stringValue(p, "key"), 64, 0, time.Now().UnixMilli())
		return messageViews(rows), err
	case "/message/queryMessagePageByTopic.query", "/dlqMessage/queryDlqMessageByConsumerGroup.query", "/message/queryMessageByTopic.query":
		return s.dashboardMessagePage(ctx, c, path, p)
	case "/message/consumeMessageDirectly.do":
		return s.dashboardConsumeDirect(ctx, c, p)
	case "/dlqMessage/batchResendDlqMessage.do", "/dlqMessage/batchExportDlqMessage.do":
		rows := []any{}
		failures := []string{}
		items, _ := p["items"].([]any)
		if len(items) == 0 {
			return nil, fmt.Errorf("select messages first")
		}
		for _, item := range items {
			entry, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("invalid message entry")
			}
			var value any
			var err error
			if path == "/dlqMessage/batchResendDlqMessage.do" {
				value, err = s.dashboardConsumeDirect(ctx, c, entry)
			} else {
				var msg *admin.MessageExt
				msg, err = s.dashboardView(ctx, c, stringValue(entry, "topicName", "topic"), stringValue(entry, "msgId"))
				if err == nil {
					value = messageView(msg)
				}
			}
			if err != nil {
				failures = append(failures, stringValue(entry, "msgId")+": "+err.Error())
				rows = append(rows, map[string]any{"msgId": stringValue(entry, "msgId"), "consumeResult": "CR_THROW_EXCEPTION", "remark": err.Error()})
			} else {
				row := jsonObject(value)
				row["msgId"] = stringValue(entry, "msgId")
				rows = append(rows, row)
			}
		}
		if len(failures) > 0 {
			return rows, fmt.Errorf("%d of %d messages failed: %s", len(failures), len(items), strings.Join(failures, "; "))
		}
		return rows, nil
	case "/messageTrace/viewMessageTraceGraph.query":
		traceTopic := valueOrDefault(stringValue(p, "traceTopic"), "RMQ_SYS_TRACE_TOPIC")
		messages, err := queryMessagesByKey(ctx, c, s.connection.ConnectTimeout, traceTopic, id, 64, 0, time.Now().UnixMilli())
		if err != nil {
			return nil, err
		}
		return buildTraceGraph(messages, id), nil
	}
	return nil, fmt.Errorf("unknown message operation %s", path)
}

func (s *session) dashboardView(ctx context.Context, c *admin.Client, topic, id string) (*admin.MessageExt, error) {
	if topic == "" || id == "" {
		return nil, fmt.Errorf("topic and msgId are required")
	}
	// Offset IDs embed store address and commit-log offset; broker port may differ
	// from store port, so resolve against this topic's trusted route first.
	raw, decodeErr := hex.DecodeString(id)
	if decodeErr == nil && (len(raw) == 16 || len(raw) == 28) {
		ipBytes := len(raw) - 12
		ip := net.IP(raw[:ipBytes]).String()
		offset := binary.BigEndian.Uint64(raw[len(raw)-8:])
		route, err := c.ExamineTopicRouteInfo(ctx, topic)
		if err != nil {
			return nil, err
		}
		for _, b := range route.BrokerDatas {
			for _, addr := range b.BrokerAddrs {
				original := valueOrDefault(s.proxies.OriginalForLocal(addr), addr)
				host, _, _ := net.SplitHostPort(original)
				if host != ip {
					continue
				}
				response, e := invokeRemotingWithClient(ctx, addr, remoting.NewRequest(remoting.ViewMessageById, map[string]string{"offset": strconv.FormatUint(offset, 10)}))
				if e != nil {
					continue
				}
				messages := decodeDashboardMessages(response.Body, b.BrokerName)
				for _, m := range messages {
					if m.Topic == topic {
						return m, nil
					}
				}
			}
		}
	}
	messages, err := queryMessagesByKey(ctx, c, s.connection.ConnectTimeout, topic, id, 64, 0, time.Now().UnixMilli())
	if err != nil {
		return nil, err
	}
	for _, m := range messages {
		if m.MsgId == id || m.OffsetMsgId == id || m.Properties["UNIQ_KEY"] == id {
			return m, nil
		}
	}
	return nil, fmt.Errorf("message not found: %s", id)
}

func decodeDashboardMessages(body []byte, broker string) []*admin.MessageExt {
	rows := []*admin.MessageExt{}
	for _, m := range primitive.DecodeMessage(body) {
		qid := 0
		if m.Queue != nil {
			qid = m.Queue.QueueId
		}
		rows = append(rows, &admin.MessageExt{StoreSize: m.StoreSize, BodyCRC: m.BodyCRC, ReconsumeTimes: m.ReconsumeTimes, PreparedTransactionOffset: m.PreparedTransactionOffset, CommitLogOffset: m.CommitLogOffset, Topic: m.Topic, QueueId: qid, QueueOffset: m.QueueOffset, MsgId: m.MsgId, OffsetMsgId: m.OffsetMsgId, Body: m.Body, Flag: int(m.Flag), BornTimestamp: m.BornTimestamp, StoreTimestamp: m.StoreTimestamp, BornHost: m.BornHost, StoreHost: m.StoreHost, SysFlag: int(m.SysFlag), BrokerName: broker, Properties: m.GetProperties()})
	}
	return rows
}

type dashboardQuery struct {
	Topic      string
	Begin, End int64
	Rows       []any
	Created    time.Time
}

func (s *session) dashboardMessagePage(ctx context.Context, c *admin.Client, path string, p map[string]any) (any, error) {
	topic := stringValue(p, "topic")
	begin := int64Value(p, 0, "begin")
	end := int64Value(p, time.Now().UnixMilli(), "end")
	page := maxValue(0, intValue(p, 1, "pageNum")-1)
	size := intValue(p, 20, "pageSize")
	if topic == "" || begin > end || page < 0 || size < 1 || size > 1000 {
		return nil, fmt.Errorf("invalid topic, time range or pagination")
	}
	state := s.dashboard
	id := stringValue(p, "taskId")
	var query dashboardQuery
	found := false
	state.mu.Lock()
	for key, q := range state.queries {
		if time.Since(q.Created) > time.Hour {
			delete(state.queries, key)
		}
	}
	if id != "" {
		query, found = state.queries[id]
	}
	state.mu.Unlock()
	if found && (query.Topic != topic || query.Begin != begin || query.End != end) {
		return nil, fmt.Errorf("query task does not match topic or time range")
	}
	if id != "" && !found {
		return nil, fmt.Errorf("query task expired; start a new search")
	}
	if !found {
		targets, err := s.messageQueueTargets(ctx, c, topic, false)
		if err != nil && !(strings.HasPrefix(topic, "%DLQ%") && rocketMQCode(err, 17)) {
			return nil, err
		}
		messages := []*admin.MessageExt{}
		for _, target := range targets {
			offset, err := c.SearchOffset(ctx, target.Address, topic, target.QueueID, begin)
			if err != nil {
				return nil, err
			}
			maxOffset, err := queueOffset(ctx, target.Address, topic, target.QueueID, remoting.GetMaxOffset)
			if err != nil {
				return nil, err
			}
			for offset < maxOffset {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				response, err := invokeRemotingAllowCodes(ctx, target.Address, s.connection.ConnectTimeout, remoting.NewRequest(remoting.PullMessage, map[string]string{"consumerGroup": "TOOLS_CONSUMER", "topic": topic, "queueId": strconv.Itoa(target.QueueID), "queueOffset": strconv.FormatInt(offset, 10), "maxMsgNums": "32", "sysFlag": "4", "subscription": "*", "expressionType": "TAG", "subVersion": "0", "commitOffset": "0", "suspendTimeoutMillis": "0"}), 0, 19, 20, 21)
				if err != nil {
					return nil, err
				}
				next, err := strconv.ParseInt(response.ExtFields["nextBeginOffset"], 10, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid nextBeginOffset: %w", err)
				}
				beyond := false
				for _, msg := range decodeDashboardMessages(response.Body, target.BrokerName) {
					if msg.StoreTimestamp > end {
						beyond = true
					}
					if msg.StoreTimestamp >= begin && msg.StoreTimestamp <= end {
						messages = append(messages, msg)
					}
				}
				if len(messages) > 50000 {
					return nil, fmt.Errorf("more than 50000 messages in range; narrow the time range")
				}
				if beyond || next <= offset || response.Code == 19 {
					break
				}
				offset = next
			}
		}
		sort.Slice(messages, func(i, j int) bool { return messages[i].StoreTimestamp > messages[j].StoreTimestamp })
		id = primitive.CreateUniqID()
		query = dashboardQuery{topic, begin, end, messageViews(messages), time.Now()}
		state.mu.Lock()
		if len(state.queries) >= 16 {
			oldest := ""
			for key, q := range state.queries {
				if oldest == "" || q.Created.Before(state.queries[oldest].Created) {
					oldest = key
				}
			}
			delete(state.queries, oldest)
		}
		state.queries[id] = query
		state.mu.Unlock()
	}
	if path == "/message/queryMessageByTopic.query" {
		return query.Rows, nil
	}
	start := minValue(page*size, len(query.Rows))
	finish := minValue(start+size, len(query.Rows))
	totalPages := (len(query.Rows) + size - 1) / size
	return map[string]any{"taskId": id, "page": map[string]any{"content": query.Rows[start:finish], "totalElements": len(query.Rows), "totalPages": totalPages, "number": page, "size": size, "first": page == 0, "last": page+1 >= totalPages, "numberOfElements": finish - start, "empty": start == finish}}, nil
}

func (s *session) dashboardConsumeDirect(ctx context.Context, c *admin.Client, p map[string]any) (any, error) {
	group := stringValue(p, "consumerGroup")
	topic := stringValue(p, "topic", "topicName")
	id := stringValue(p, "msgId")
	clientID := stringValue(p, "clientId")
	if group == "" || topic == "" || id == "" {
		return nil, fmt.Errorf("consumerGroup, topic and msgId are required")
	}
	if clientID == "" {
		conn, err := s.dashboardConnection(ctx, c, group, "")
		if err != nil {
			return nil, err
		}
		if len(conn.ConnectionSet) == 0 {
			return nil, fmt.Errorf("consumer group is offline")
		}
		clientID = conn.ConnectionSet[0].ClientId
	}
	msg, err := s.dashboardView(ctx, c, topic, id)
	if err != nil {
		return nil, err
	}
	route, err := c.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		return nil, err
	}
	for _, b := range route.BrokerDatas {
		if msg.BrokerName != "" && b.BrokerName != msg.BrokerName {
			continue
		}
		addr := b.BrokerAddrs["0"]
		if addr == "" {
			continue
		}
		response, e := invokeRemotingWithClient(ctx, addr, remoting.NewRequest(309, map[string]string{"consumerGroup": group, "clientId": clientID, "msgId": valueOrDefault(msg.OffsetMsgId, msg.MsgId), "topic": topic, "brokerName": b.BrokerName}))
		if e != nil {
			return nil, e
		}
		var result map[string]any
		e = json.Unmarshal(response.Body, &result)
		if code, ok := result["consumeResult"].(float64); ok {
			values := []string{"CR_SUCCESS", "CR_LATER", "CR_ROLLBACK", "CR_COMMIT", "CR_THROW_EXCEPTION", "CR_RETURN_NULL"}
			if code >= 0 && int(code) < len(values) {
				result["consumeResult"] = values[int(code)]
			} else {
				return nil, fmt.Errorf("unknown consumeResult %v", code)
			}
		}
		if e == nil && result["consumeResult"] != "CR_SUCCESS" && result["consumeResult"] != "CR_COMMIT" {
			e = fmt.Errorf("consumer returned %v: %v", result["consumeResult"], result["remark"])
		}
		return result, e
	}
	return nil, fmt.Errorf("message broker is unavailable")
}

type commitTransaction struct{}

func (commitTransaction) ExecuteLocalTransaction(*primitive.Message) primitive.LocalTransactionState {
	return primitive.CommitMessageState
}
func (commitTransaction) CheckLocalTransaction(*primitive.MessageExt) primitive.LocalTransactionState {
	return primitive.CommitMessageState
}

func (s *session) dashboardSend(ctx context.Context, c *admin.Client, p map[string]any) (any, error) {
	topic := stringValue(p, "topic")
	if topic == "" {
		return nil, fmt.Errorf("topic is required")
	}
	route, err := c.ExamineTopicRouteInfo(ctx, topic)
	if err != nil {
		return nil, err
	}
	if len(route.BrokerDatas) == 0 {
		return nil, fmt.Errorf("no broker for topic")
	}
	cfg, err := readTopicConfig(ctx, route.BrokerDatas[0].BrokerAddrs["0"], topic)
	if err != nil {
		return nil, err
	}
	opts := []producer.Option{producer.WithNameServer(primitive.NamesrvAddr(c.GetNameServerAddressList())), producer.WithGroupName("DBX_DASHBOARD_PRODUCER"), producer.WithInstanceName(primitive.CreateUniqID()), producer.WithVIPChannel(false), producer.WithRetry(0)}
	body, ok := p["messageBody"].(string)
	if !ok {
		return nil, fmt.Errorf("messageBody must be a string")
	}
	msg := primitive.NewMessage(topic, []byte(body))
	msg.WithKeys([]string{stringValue(p, "key")})
	msg.WithTag(stringValue(p, "tag"))
	for key, value := range nestedMap(p, "properties") {
		if key != "UNIQ_KEY" && key != "TRAN_MSG" {
			msg.WithProperty(key, fmt.Sprint(value))
		}
	}
	var result *primitive.SendResult
	started := time.Now()
	if cfg.Attributes["message.type"] == "TRANSACTION" {
		prod, e := rocketmq.NewTransactionProducer(commitTransaction{}, opts...)
		if e != nil {
			return nil, e
		}
		if e = prod.Start(); e != nil {
			return nil, e
		}
		defer prod.Shutdown()
		tx, e := prod.SendMessageInTransaction(ctx, msg)
		if e != nil {
			return nil, e
		}
		result = tx.SendResult
	} else {
		prod, e := rocketmq.NewProducer(opts...)
		if e != nil {
			return nil, e
		}
		if e = prod.Start(); e != nil {
			return nil, e
		}
		defer prod.Shutdown()
		result, err = prod.SendSync(ctx, msg)
		if err != nil {
			return nil, err
		}
	}
	status := []string{"SEND_OK", "FLUSH_DISK_TIMEOUT", "FLUSH_SLAVE_TIMEOUT", "SLAVE_NOT_AVAILABLE"}
	code := int(result.Status)
	if code < 0 || code >= len(status) {
		return nil, fmt.Errorf("unknown send status %d", code)
	}
	data := map[string]any{"sendStatus": status[code], "msgId": result.MsgID, "offsetMsgId": result.OffsetMsgID, "messageQueue": result.MessageQueue, "queueOffset": result.QueueOffset, "transactionId": result.TransactionID}
	if boolValue(p, false, "traceEnabled") {
		if e := s.dashboardSendTrace(ctx, c, p, result, started, time.Since(started), cfg.Attributes["message.type"]); e != nil {
			return data, fmt.Errorf("message %s was sent; trace publication failed: %w", result.MsgID, e)
		}
	}
	return data, nil
}

// Synchronous trace publication avoids the Go SDK's process-global trace
// client and its non-flushing Close, keeping credentials and routes isolated.
func (s *session) dashboardSendTrace(ctx context.Context, c *admin.Client, p map[string]any, result *primitive.SendResult, started time.Time, cost time.Duration, kind string) error {
	traceTopic := valueOrDefault(stringValue(p, "traceTopic"), "RMQ_SYS_TRACE_TOPIC")
	store := ""
	if raw, e := hex.DecodeString(result.OffsetMsgID); e == nil && (len(raw) == 16 || len(raw) == 28) {
		n := len(raw) - 12
		store = net.JoinHostPort(net.IP(raw[:n]).String(), strconv.FormatUint(uint64(binary.BigEndian.Uint32(raw[n:n+4])), 10))
	}
	typeID := "0"
	if kind == "TRANSACTION" {
		typeID = "1"
	}
	if kind == "DELAY" {
		typeID = "3"
	}
	body, _ := p["messageBody"].(string)
	fields := []string{"Pub", strconv.FormatInt(started.UnixMilli(), 10), "DefaultRegion", "DBX_DASHBOARD_PRODUCER", stringValue(p, "topic"), result.MsgID, stringValue(p, "tag"), stringValue(p, "key"), store, strconv.Itoa(len([]byte(body))), strconv.FormatInt(cost.Milliseconds(), 10), typeID, result.OffsetMsgID, strconv.FormatBool(result.Status == primitive.SendOK)}
	record := strings.Join(fields, "\x01") + "\x02"
	if kind == "TRANSACTION" {
		record += strings.Join([]string{"EndTransaction", strconv.FormatInt(time.Now().UnixMilli(), 10), "DefaultRegion", "DBX_DASHBOARD_PRODUCER", stringValue(p, "topic"), result.MsgID, stringValue(p, "tag"), stringValue(p, "key"), store, "2", result.TransactionID, "COMMIT_MESSAGE", "false"}, "\x01") + "\x02"
	}
	prod, e := rocketmq.NewProducer(producer.WithNameServer(primitive.NamesrvAddr(c.GetNameServerAddressList())), producer.WithGroupName("DBX_DASHBOARD_TRACE"), producer.WithInstanceName(primitive.CreateUniqID()), producer.WithVIPChannel(false), producer.WithRetry(0))
	if e != nil {
		return e
	}
	if e = prod.Start(); e != nil {
		return e
	}
	defer prod.Shutdown()
	m := primitive.NewMessage(traceTopic, []byte(record))
	m.WithKeys([]string{result.MsgID})
	sent, e := prod.SendSync(ctx, m)
	if e == nil && sent.Status != primitive.SendOK {
		return fmt.Errorf("trace send status %v", sent.Status)
	}
	return e
}
