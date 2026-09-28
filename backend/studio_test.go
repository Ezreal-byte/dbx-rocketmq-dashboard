package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	rocketmq "github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func studioCall(t *testing.T, s *session, path string, p map[string]any) any {
	t.Helper()
	pattern, _ := studioRoute(path)
	route := studioRoutes[pattern]
	r := s.studioRequest(map[string]any{"method": route.Method, "path": path, "body": jsonObject(p)})
	if r["code"] != 0 {
		t.Fatalf("%s: %v", path, r["message"])
	}
	t.Logf("PASS %s", path)
	return jsonObject(map[string]any{"value": r["data"]})["value"]
}
func TestStudioGuards(t *testing.T) {
	s := &session{readOnly: true}
	for path, route := range studioRoutes {
		if !route.Write {
			continue
		}
		r := s.studioRequest(map[string]any{"method": route.Method, "path": path})
		if r["code"] == 0 || !strings.Contains(fmt.Sprint(r["message"]), "read-only") {
			t.Fatalf("write guard %s: %v", path, r)
		}
	}
	for _, path := range []string{"https://example.test", "/../groups", "/groups//settings", "/auth/login", "/ai/chat"} {
		r := s.studioRequest(map[string]any{"path": path, "method": "GET"})
		if r["code"] == 0 {
			t.Fatal(path)
		}
	}
	pattern, p := studioRoute("/groups/%25RETRY%25demo/progress")
	if pattern != "/groups/:id/progress" || p["id"] != "%RETRY%demo" {
		t.Fatal(pattern, p)
	}
}
func TestStudioIntegration(t *testing.T) {
	if os.Getenv("ROCKETMQ_INTEGRATION") != "1" {
		t.Skip("isolated RocketMQ required")
	}
	t.Setenv("DBX_PLUGIN_DATA_DIR", t.TempDir())
	s, e := newSession(lifecycleParams{Connection: connection{ID: "studio-integration", Name: "Studio integration", Config: externalConfig{NamesrvAddr: os.Getenv("ROCKETMQ_NAMESRV_ADDR")}}})
	if e != nil {
		t.Fatal(e)
	}
	defer s.close()
	if e = s.probe(); e != nil {
		t.Fatal(e)
	}
	topic := fmt.Sprintf("DBX_STUDIO_%d", time.Now().UnixNano())
	group := "GID_" + topic
	studioCall(t, s, "/topics/create", map[string]any{"name": topic, "type": "NORMAL", "readQueues": 2, "writeQueues": 2, "perm": "RW"})
	defer studioCall(t, s, "/topics/delete", map[string]any{"name": topic})
	studioCall(t, s, "/groups/create", map[string]any{"name": group, "retryMaxTimes": 12})
	defer studioCall(t, s, "/groups/delete", map[string]any{"name": group})
	for _, path := range []string{"/instances", "/instances/1/capabilities", "/dashboard", "/clusters", "/clusters/registry", "/nameservers", "/producer/groups", "/producer/connection", "/clients", "/metrics/profiles"} {
		studioCall(t, s, path, nil)
	}
	for _, path := range []string{"/topics", "/topics/page", "/topics/export", "/topics/" + topic + "/routes", "/topics/" + topic + "/consumers", "/topics/" + topic + "/consumers/page"} {
		studioCall(t, s, path, map[string]any{"search": topic, "page": 1, "pageSize": 10})
	}
	for _, suffix := range []string{"", "/refresh", "/settings", "/subscriptions", "/progress"} {
		studioCall(t, s, "/groups/"+group+suffix, nil)
	}
	for _, path := range []string{"/groups", "/groups/page", "/groups/export"} {
		studioCall(t, s, path, map[string]any{"search": group})
	}
	studioCall(t, s, "/groups/settings", map[string]any{"name": group, "retryMaxTimes": 9, "retryQueueNums": 1})
	// Use a real subscribed consumer and real messages for broker-mediated reset.
	client, _, _ := s.requireClient()
	sub, err := rocketmq.NewPushConsumer(consumer.WithNameServer(primitive.NamesrvAddr(client.GetNameServerAddressList())), consumer.WithGroupName(group), consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset))
	if err != nil {
		t.Fatal(err)
	}
	delivered := make(chan struct{}, 1)
	if err = sub.Subscribe(topic, consumer.MessageSelector{Type: consumer.TAG, Expression: "*"}, func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		select {
		case delivered <- struct{}{}:
		default:
		}
		return consumer.ConsumeSuccess, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = sub.Start(); err != nil {
		t.Fatal(err)
	}
	defer sub.Shutdown()
	text := "Studio 原生界面\nGo backend test"
	send := studioCall(t, s, "/topics/send", map[string]any{"topic": topic, "body": text, "key": topic, "tag": "studio", "properties": map[string]any{"custom": "yes"}}).(map[string]any)
	select {
	case <-delivered:
	case <-time.After(30 * time.Second):
		t.Fatal("Studio fixture consumer did not receive the real message")
	}
	q := map[string]any{"topic": topic, "msgId": send["msgId"]}
	rows := studioCall(t, s, "/messages", q).([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["body"] != text {
		t.Fatalf("message data %v", rows)
	}
	row := rows[0].(map[string]any)
	if nestedMap(row, "properties")["custom"] != "yes" {
		t.Fatalf("properties lost: %v", row)
	}
	studioCall(t, s, "/messages/page", q)
	studioCall(t, s, "/messages/queues", q)
	studioCall(t, s, "/messages/queue-message", map[string]any{"topic": topic, "brokerName": row["brokerName"], "queueId": row["queueId"], "offset": row["queueOffset"]})
	// 4.9 requires persisted offsets; do not depend on the SDK's periodic flush timer.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	targets, err := s.messageQueueTargets(ctx, client, topic, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range targets {
		if err = client.UpdateConsumeOffset(ctx, target.Address, group, topic, target.QueueID, 0); err != nil {
			t.Fatal(err)
		}
	}
	studioCall(t, s, "/groups/reset-offset/preview", map[string]any{"name": group, "topic": topic, "timestamp": time.Now().UnixMilli()})
	studioCall(t, s, "/groups/reset-offset", map[string]any{"name": group, "topic": topic, "timestamp": time.Now().UnixMilli()})
	studioCall(t, s, "/dlq", nil)
	for _, path := range []string{"/clusters/" + s.clusterName, "/clusters/" + s.clusterName + "/broker-config-diff", "/nameservers/config-diff"} {
		studioCall(t, s, path, map[string]any{"clusterId": s.clusterName})
	}
	if os.Getenv("ROCKETMQ_VERSION") == "5.3.3" {
		studioCall(t, s, "/acl/users/page", nil)
		studioCall(t, s, "/acl/rules", nil)
	}
	r := s.studioRequest(map[string]any{"method": "GET", "path": "/topics", "query": map[string]any{"instanceId": "2"}})
	if r["code"] == 0 {
		t.Fatal("foreign instance accepted")
	}
	r = s.studioRequest(map[string]any{"method": "GET", "path": "/groups/page", "query": map[string]any{"instanceId": "Studio integration"}})
	if r["code"] != 0 {
		t.Fatalf("upstream UI uses instance name: %v", r)
	}
}
