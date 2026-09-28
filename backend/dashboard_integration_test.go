package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func dashboardCall(t *testing.T, s *session, path string, body map[string]any) any {
	t.Helper()
	if body != nil {
		body = jsonObject(body)
	}
	r := s.dashboardRequest(map[string]any{"method": dashboardRoutes[path].Method, "path": path, "body": body})
	if r["status"] != 0 {
		t.Fatalf("%s: %v", path, r["errMsg"])
	}
	b, e := json.Marshal(r["data"])
	if e != nil {
		t.Fatal(e)
	}
	var data any
	if e = json.Unmarshal(b, &data); e != nil {
		t.Fatal(e)
	}
	t.Logf("PASS %s", path)
	return data
}

func TestDashboardIntegration(t *testing.T) {
	if os.Getenv("ROCKETMQ_INTEGRATION") != "1" {
		t.Skip("requires isolated RocketMQ broker")
	}
	t.Setenv("DBX_PLUGIN_DATA_DIR", t.TempDir())
	version := envString("ROCKETMQ_VERSION", "unknown")
	s, err := newSession(lifecycleParams{Connection: connection{ID: "dashboard-integration", Config: externalConfig{NamesrvAddr: os.Getenv("ROCKETMQ_NAMESRV_ADDR")}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.close)
	if err = s.probe(); err != nil {
		t.Fatal(err)
	}
	cluster := dashboardCall(t, s, "/cluster/list.query", nil).(map[string]any)
	brokers := cluster["clusterInfo"].(map[string]any)["brokerAddrTable"].(map[string]any)
	var broker string
	for name := range brokers {
		broker = name
		break
	}
	addr := brokers[broker].(map[string]any)["brokerAddrs"].(map[string]any)["0"].(string)
	dashboardCall(t, s, "/cluster/brokerConfig.query", map[string]any{"brokerAddr": addr})
	name := fmt.Sprintf("DBX_DASHBOARD_%s_%d", strings.ReplaceAll(version, ".", "_"), time.Now().UnixNano())
	group := "GID_" + name
	t.Cleanup(func() {
		dashboardCall(t, s, "/consumer/deleteSubGroup.do", map[string]any{"groupName": group, "brokerNameList": []string{broker}})
		dashboardCall(t, s, "/topic/deleteTopic.do", map[string]any{"topic": name})
	})
	dashboardCall(t, s, "/topic/createOrUpdate.do", map[string]any{"topicName": name, "readQueueNums": 2, "writeQueueNums": 2, "brokerNameList": []string{broker}, "messageType": "NORMAL"})
	for _, path := range []string{"/topic/list.query", "/topic/list.queryTopicType", "/topic/route.query", "/topic/stats.query", "/topic/examineTopicConfig.query", "/topic/queryTopicConsumerInfo.query", "/topic/queryConsumerByTopic.query"} {
		dashboardCall(t, s, path, map[string]any{"topic": name})
	}
	dashboardCall(t, s, "/consumer/createOrUpdate.do", map[string]any{"brokerNameList": []string{broker}, "subscriptionGroupConfig": map[string]any{"groupName": group, "consumeEnable": true, "consumeBroadcastEnable": true, "retryQueueNums": 1, "retryMaxTimes": 16}})
	for _, path := range []string{"/consumer/examineSubscriptionGroupConfig.query", "/consumer/fetchBrokerNameList.query", "/consumer/queryTopicByConsumer.query", "/consumer/group.query", "/consumer/groupList.query", "/consumer/group.refresh", "/consumer/group.refresh.all"} {
		dashboardCall(t, s, path, map[string]any{"consumerGroup": group})
	}
	begin := time.Now().Add(-time.Minute).UnixMilli()
	body := "  RocketMQ 世界\n dashboard " + version + "  \n"
	send := dashboardCall(t, s, "/topic/sendTopicMessage.do", map[string]any{"topic": name, "messageBody": body, "key": name, "tag": "dashboard"}).(map[string]any)
	if send["sendStatus"] != "SEND_OK" {
		t.Fatalf("send=%v", send)
	}
	// Index and consume queue updates are asynchronous on the broker.
	var paged map[string]any
	var pageError any
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		r := s.dashboardRequest(map[string]any{"method": "POST", "path": "/message/queryMessagePageByTopic.query", "body": jsonObject(map[string]any{"topic": name, "begin": begin, "end": time.Now().UnixMilli(), "pageNum": 1, "pageSize": 1})})
		pageError = r["errMsg"]
		if r["status"] == 0 {
			paged = jsonObject(r["data"])
			if paged["page"].(map[string]any)["totalElements"].(float64) > 0 {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if paged == nil {
		t.Fatalf("message page unavailable: %v", pageError)
	}
	page := paged["page"].(map[string]any)
	rows := page["content"].([]any)
	if len(rows) != 1 || page["number"] != float64(0) {
		t.Fatalf("pagination=%v", page)
	}
	msg := rows[0].(map[string]any)
	if msg["messageBody"] != body {
		t.Fatalf("body corrupted: %q", msg["messageBody"])
	}
	if msg["storeSize"].(float64) <= 0 || msg["bodyCRC"].(float64) == 0 || msg["reconsumeTimes"] != float64(0) {
		t.Fatalf("message metadata lost: %v", msg)
	}
	t.Log("PASS message paging preserves body and 1-based request / 0-based response")
	dashboardCall(t, s, "/message/viewMessage.query", map[string]any{"topic": name, "msgId": msg["msgId"]})
	dashboardCall(t, s, "/messageTrace/viewMessage.query", map[string]any{"topic": name, "msgId": msg["msgId"]})
	dashboardCall(t, s, "/message/queryMessageByTopic.query", map[string]any{"topic": name, "begin": begin, "end": time.Now().UnixMilli()})
	keys := dashboardCall(t, s, "/message/queryMessageByTopicAndKey.query", map[string]any{"topic": name, "key": name}).([]any)
	if len(keys) != 1 {
		t.Fatalf("key query=%v", keys)
	}
	// Real offline offsets; reset must modify stored offsets and return queue rows.
	c, _, _ := s.requireClient()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	local, err := s.dashboardAddress(ctx, c, addr)
	if err != nil {
		t.Fatal(err)
	}
	for q := 0; q < 2; q++ {
		if err = c.UpdateConsumeOffset(ctx, local, group, name, q, 0); err != nil {
			t.Fatal(err)
		}
	}
	reset := dashboardCall(t, s, "/consumer/resetOffset.do", map[string]any{"topic": name, "consumerGroupList": []string{group}, "resetTime": -1, "force": true}).(map[string]any)[group].(map[string]any)
	if reset["status"] != true || len(reset["rollbackStatsList"].([]any)) != 2 {
		t.Fatalf("reset=%v", reset)
	}
	stats, err := s.dashboardRawStats(ctx, c, group, name)
	if err != nil {
		t.Fatal(err)
	}
	var consumed int64
	for _, o := range stats.OffsetTable {
		consumed += o.ConsumerOffset
	}
	if consumed != 1 {
		t.Fatalf("reset did not change broker offsets: %d", consumed)
	}
	dashboardCall(t, s, "/consumer/queryTopicByConsumer.query", map[string]any{"consumerGroup": group})
	dashboardCall(t, s, "/consumer/skipAccumulate.do", map[string]any{"topic": name, "consumerGroupList": []string{group}, "force": true})
	dashboardCall(t, s, "/monitor/createOrUpdateConsumerMonitor.do", map[string]any{"consumeGroupName": group, "minCount": 1, "maxDiffTotal": 10})
	dashboardCall(t, s, "/monitor/consumerMonitorConfigByGroupName.query", map[string]any{"consumeGroupName": group})
	dashboardCall(t, s, "/monitor/consumerMonitorConfig.query", nil)
	dashboardCall(t, s, "/monitor/deleteConsumerMonitor.do", map[string]any{"consumeGroupName": group})
	s.collectDashboard(ctx)
	for _, path := range []string{"/dashboard/broker.query", "/dashboard/topic.query", "/dashboard/topicCurrent.query", "/proxy/homePage.query"} {
		dashboardCall(t, s, path, map[string]any{"topicName": name})
	}
	dashboardCall(t, s, "/topic/deleteTopicByBroker.do", map[string]any{"topic": name, "brokerName": broker})
	s.readOnly = true
	for path, route := range dashboardRoutes {
		if route.Write {
			r := s.dashboardRequest(map[string]any{"path": path, "method": route.Method})
			if r["status"] == 0 {
				t.Errorf("read-only allowed %s", path)
			}
		}
	}
	s.readOnly = false
	t.Log("PASS every registered mutation denied in read-only mode")
}
