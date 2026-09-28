package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
	rocketmq "github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

func TestDashboardLiveIntegration(t *testing.T) {
	if os.Getenv("ROCKETMQ_INTEGRATION") != "1" {
		t.Skip("requires isolated broker")
	}
	t.Setenv("DBX_PLUGIN_DATA_DIR", t.TempDir())
	s, err := newSession(lifecycleParams{Connection: connection{ID: "live", Config: externalConfig{NamesrvAddr: os.Getenv("ROCKETMQ_NAMESRV_ADDR")}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.close)
	if err = s.probe(); err != nil {
		t.Fatal(err)
	}
	c, _, _ := s.requireClient()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	brokers, err := s.dashboardBrokers(ctx, c, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	broker := sortedMapKeys(brokers)[0]
	addr := brokers[broker]
	topic := fmt.Sprintf("DBX_LIVE_%d", time.Now().UnixNano())
	group := "G_" + topic
	pg := "P_" + topic
	t.Cleanup(func() {
		for _, name := range []string{topic, "TRACE_" + topic} {
			dashboardCall(t, s, "/topic/deleteTopic.do", map[string]any{"topic": name})
		}
		dashboardCall(t, s, "/consumer/deleteSubGroup.do", map[string]any{"groupName": group, "brokerNameList": []string{broker}})
	})
	dashboardCall(t, s, "/topic/createOrUpdate.do", map[string]any{"topicName": topic, "brokerNameList": []string{broker}, "readQueueNums": 1, "writeQueueNums": 1, "messageType": "NORMAL"})
	dashboardCall(t, s, "/consumer/createOrUpdate.do", map[string]any{"brokerNameList": []string{broker}, "subscriptionGroupConfig": map[string]any{"groupName": group}})
	delivered := make(chan *primitive.MessageExt, 16)
	// Go SDK 2.1.2 does not remap DLQ to RETRY_TOPIC in direct consumption.
	// Register its explicit callback so the test exercises successful dispatch.
	dashboardCall(t, s, "/topic/createOrUpdate.do", map[string]any{"topicName": "%DLQ%" + group, "brokerNameList": []string{broker}, "readQueueNums": 1, "writeQueueNums": 1, "messageType": "NORMAL"})
	sub, err := rocketmq.NewPushConsumer(consumer.WithNameServer(primitive.NamesrvAddr(c.GetNameServerAddressList())), consumer.WithGroupName(group), consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset))
	if err != nil {
		t.Fatal(err)
	}
	if err = sub.Subscribe(topic, consumer.MessageSelector{Type: consumer.TAG, Expression: "*"}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, m := range msgs {
			select {
			case delivered <- m:
			default:
			}
		}
		return consumer.ConsumeSuccess, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = sub.Subscribe("%DLQ%"+group, consumer.MessageSelector{Type: consumer.TAG, Expression: "*"}, func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		return consumer.ConsumeSuccess, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = sub.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sub.Shutdown() })
	prod, err := rocketmq.NewProducer(producer.WithNameServer(primitive.NamesrvAddr(c.GetNameServerAddressList())), producer.WithGroupName(pg), producer.WithVIPChannel(false))
	if err != nil {
		t.Fatal(err)
	}
	if err = prod.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { prod.Shutdown() })
	sent, err := prod.SendSync(ctx, primitive.NewMessage(topic, []byte("live message")))
	if err != nil {
		t.Fatal(err)
	}
	var received *primitive.MessageExt
	select {
	case received = <-delivered:
	case <-time.After(30 * time.Second):
		t.Fatal("consumer did not receive message")
	}
	t.Log("PASS live Go consumer received broker message", received.MsgId)
	conns := dashboardCall(t, s, "/consumer/consumerConnection.query", map[string]any{"consumerGroup": group, "address": s.proxies.OriginalForLocal(addr)}).(map[string]any)
	clientID := conns["connectionSet"].([]any)[0].(map[string]any)["clientId"]
	dashboardCall(t, s, "/consumer/consumerRunningInfo.query", map[string]any{"consumerGroup": group, "clientId": clientID, "jstack": true})
	dashboardCall(t, s, "/producer/producerConnection.query", map[string]any{"producerGroup": pg, "topic": topic})
	direct := dashboardCall(t, s, "/message/consumeMessageDirectly.do", map[string]any{"topic": topic, "consumerGroup": group, "clientId": clientID, "msgId": sent.OffsetMsgID}).(map[string]any)
	if direct["consumeResult"] != "CR_SUCCESS" {
		t.Fatalf("direct consume=%v", direct)
	}
	// Ask the broker to send this actual stored message to the dead-letter queue.
	raw, err := hex.DecodeString(sent.OffsetMsgID)
	if err != nil || len(raw) < 8 {
		t.Fatalf("offset id: %v", err)
	}
	_, err = invokeRemotingWithClient(ctx, addr, remoting.NewRequest(36, map[string]string{"group": group, "offset": strconv.FormatUint(binary.BigEndian.Uint64(raw[len(raw)-8:]), 10), "delayLevel": "-1", "originMsgId": sent.MsgID, "originTopic": topic, "unitMode": "false", "maxReconsumeTimes": "1"}))
	if err != nil {
		t.Fatal(err)
	}
	var dlq map[string]any
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		r := s.dashboardRequest(map[string]any{"path": "/dlqMessage/queryDlqMessageByConsumerGroup.query", "method": "POST", "body": jsonObject(map[string]any{"topic": "%DLQ%" + group, "begin": time.Now().Add(-time.Hour).UnixMilli(), "end": time.Now().UnixMilli(), "pageNum": 1, "pageSize": 20})})
		if r["status"] == 0 {
			page := jsonObject(r["data"])["page"].(map[string]any)
			if rows := page["content"].([]any); len(rows) > 0 {
				dlq = rows[0].(map[string]any)
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if dlq == nil {
		t.Fatal("dead-letter message was not queryable")
	}
	t.Log("PASS /dlqMessage/queryDlqMessageByConsumerGroup.query")
	dashboardCall(t, s, "/dlqMessage/exportDlqMessage.do", map[string]any{"consumerGroup": group, "msgId": dlq["msgId"]})
	exported := s.dashboardRequest(map[string]any{"path": "/dlqMessage/batchExportDlqMessage.do", "method": "POST", "body": []any{map[string]any{"topicName": "%DLQ%" + group, "consumerGroup": group, "msgId": dlq["msgId"]}}})
	if exported["status"] != 0 {
		t.Fatal(exported)
	}
	if exported["data"].([]any)[0].(map[string]any)["messageBody"] != dlq["messageBody"] {
		t.Fatal("batch export changed body")
	}
	t.Log("PASS /dlqMessage/batchExportDlqMessage.do")
	r := s.dashboardRequest(map[string]any{"path": "/dlqMessage/batchResendDlqMessage.do", "method": "POST", "body": []any{map[string]any{"topicName": "%DLQ%" + group, "consumerGroup": group, "clientId": clientID, "msgId": dlq["msgId"]}}})
	if r["status"] != 0 {
		t.Fatal(r)
	}
	rows := r["data"].([]any)
	if rows[0].(map[string]any)["consumeResult"] != "CR_SUCCESS" {
		t.Fatalf("batch resend=%v", r)
	}
	t.Log("PASS /dlqMessage/batchResendDlqMessage.do")
	t.Log("PASS actual DLQ query, export and consumer resend")
	// 4.9 requires an already persisted offset even for an online consumer.
	if err = c.UpdateConsumeOffset(ctx, addr, group, topic, 0, 1); err != nil {
		t.Fatal(err)
	}
	reset := dashboardCall(t, s, "/consumer/resetOffset.do", map[string]any{"topic": topic, "consumerGroupList": []string{group}, "resetTime": -1, "force": true}).(map[string]any)[group].(map[string]any)
	if reset["status"] != true {
		t.Fatalf("online reset=%v", reset)
	}
	// Trace uses a real client-generated Pub record written to the trace topic.
	dashboardCall(t, s, "/topic/createOrUpdate.do", map[string]any{"topicName": "TRACE_" + topic, "brokerNameList": []string{broker}, "readQueueNums": 1, "writeQueueNums": 1, "messageType": "NORMAL"})
	traceProd, err := rocketmq.NewProducer(producer.WithNameServer(primitive.NamesrvAddr(c.GetNameServerAddressList())), producer.WithGroupName("TRACE_"+topic), producer.WithVIPChannel(false), producer.WithTrace(&primitive.TraceConfig{TraceTopic: "TRACE_" + topic, NamesrvAddrs: c.GetNameServerAddressList()}))
	if err != nil {
		t.Fatal(err)
	}
	if err = traceProd.Start(); err != nil {
		t.Fatal(err)
	}
	defer traceProd.Shutdown()
	traced, err := traceProd.SendSync(ctx, primitive.NewMessage(topic, []byte("trace message")))
	if err != nil {
		t.Fatal(err)
	}
	var graph map[string]any
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		r := s.dashboardRequest(map[string]any{"method": "GET", "path": "/messageTrace/viewMessageTraceGraph.query", "query": map[string]any{"msgId": traced.MsgID, "traceTopic": "TRACE_" + topic}})
		if r["status"] == 0 {
			g := jsonObject(r["data"])
			if g["producerNode"] != nil {
				graph = g
				break
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	if graph == nil {
		t.Fatal("real producer trace not found")
	}
	node := graph["producerNode"].(map[string]any)
	if node["traceNode"] == nil || node["topic"] != topic {
		t.Fatalf("trace contract=%v", node)
	}
	t.Log("PASS real producer trace graph contract")
	pluginSent := dashboardCall(t, s, "/topic/sendTopicMessage.do", map[string]any{"topic": topic, "messageBody": "plugin trace", "traceEnabled": true, "traceTopic": "TRACE_" + topic}).(map[string]any)
	pluginGraph := dashboardCall(t, s, "/messageTrace/viewMessageTraceGraph.query", map[string]any{"msgId": pluginSent["msgId"], "traceTopic": "TRACE_" + topic}).(map[string]any)
	if pluginGraph["producerNode"] == nil {
		t.Fatal("plugin trace not persisted before send returned")
	}
	if strings.HasPrefix(os.Getenv("ROCKETMQ_VERSION"), "5.") {
		user := "dbx-test-" + topic
		dashboardCall(t, s, "/acl/createUser.do", map[string]any{"brokerName": broker, "userInfo": map[string]any{"username": user, "password": "isolated-test-only", "userType": "Normal", "userStatus": "Enable"}})
		t.Cleanup(func() {
			dashboardCall(t, s, "/acl/deleteUser.do", map[string]any{"brokerName": broker, "username": user})
		})
		dashboardCall(t, s, "/acl/users.query", map[string]any{"brokerName": broker})
		dashboardCall(t, s, "/acl/updateUser.do", map[string]any{"brokerName": broker, "userInfo": map[string]any{"username": user, "password": "changed-test-only", "userType": "Normal", "userStatus": "Enable"}})
		for _, action := range []string{"createAcl", "updateAcl"} {
			dashboardCall(t, s, "/acl/"+action+".do", map[string]any{"brokerName": broker, "subject": "User:" + user, "policies": []any{map[string]any{"policyType": "Custom", "entries": []any{map[string]any{"resource": []string{"Topic:" + topic}, "actions": []string{"Pub", "Sub"}, "sourceIps": []string{"127.0.0.1/32"}, "decision": "Allow"}}}}})
		}
		dashboardCall(t, s, "/acl/acls.query", map[string]any{"brokerName": broker, "searchParam": user})
		dashboardCall(t, s, "/acl/deleteAcl.do", map[string]any{"brokerName": broker, "subject": "User:" + user, "resource": "Topic:" + topic})
	} else {
		r := s.dashboardRequest(map[string]any{"path": "/acl/users.query", "method": "GET", "query": map[string]any{"brokerName": broker}})
		if r["status"] == 0 {
			t.Fatal("4.9 incorrectly accepted ACL2 query")
		}
		t.Log("PASS 4.9 ACL2 unsupported returned real Broker error")
	}
}
