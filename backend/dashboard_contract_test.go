package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	admin "github.com/amigoer/rocketmq-admin-go"
	"strings"
	"testing"
	"time"
)

func TestDashboardAllowlistAndReadOnly(t *testing.T) {
	s := &session{readOnly: true}
	for path, route := range dashboardRoutes {
		r := s.dashboardRequest(map[string]any{"path": path, "method": "PATCH"})
		if r["status"] == 0 || !strings.Contains(fmt.Sprint(r["errMsg"]), "method") {
			t.Fatalf("method not rejected: %s %v", path, r)
		}
		if route.Write {
			r = s.dashboardRequest(map[string]any{"path": path, "method": route.Method})
			if r["status"] == 0 || !strings.Contains(strings.ToLower(fmt.Sprint(r["errMsg"])), "read") {
				t.Fatalf("readonly: %s %v", path, r)
			}
		}
	}
	r := s.dashboardRequest(map[string]any{"path": "/ops/updateNameSvrAddr.do", "method": "POST"})
	if r["status"] == 0 {
		t.Fatal("unregistered endpoint allowed")
	}
}
func TestDashboardPartialFailureIsNotSuccess(t *testing.T) {
	_, err := mutateBrokers(map[string]string{"brokerA": "a", "brokerB": "b"}, func(addr string) error {
		if addr == "b" {
			return errors.New("permission denied")
		}
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "brokerA") || !strings.Contains(err.Error(), "brokerB") || !strings.Contains(err.Error(), "permission denied") {
		t.Fatal(err)
	}
	r := dashboardResult([]string{"partial"}, err)
	if r["status"] == 0 || r["data"] == nil {
		t.Fatal("lost partial failure information")
	}
}
func TestDashboardDownloadIsolationAndClose(t *testing.T) {
	t.Setenv("DBX_PLUGIN_DATA_DIR", t.TempDir())
	a := &session{dashboard: newDashboardState("A")}
	b := &session{dashboard: newDashboardState("B")}
	payload := "  世界\n" + strings.Repeat("x", 300000)
	if _, err := a.dashboardDownload("filesystem/download/open", map[string]any{"downloadId": "same-id", "content": payload}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.dashboardDownload("filesystem/download/read", map[string]any{"downloadId": "same-id"}); err == nil {
		t.Fatal("cross-connection download allowed")
	}
	var result []byte
	for {
		v, e := a.dashboardDownload("filesystem/download/read", map[string]any{"downloadId": "same-id"})
		if e != nil {
			t.Fatal(e)
		}
		m := v.(map[string]any)
		part, e := base64.StdEncoding.DecodeString(m["dataBase64"].(string))
		if e != nil {
			t.Fatal(e)
		}
		result = append(result, part...)
		if m["done"] == true {
			break
		}
	}
	if string(result) != payload {
		t.Fatal("download changed byte content")
	}
	a.close()
	if len(a.dashboard.downloads) != 0 || len(a.dashboard.queries) != 0 {
		t.Fatal("disconnect left session data")
	}
}

func TestDashboardStagedExport(t *testing.T) {
	t.Setenv("DBX_PLUGIN_DATA_DIR", t.TempDir())
	s := &session{dashboard: newDashboardState("staged")}
	call := func(method string, p map[string]any) any {
		t.Helper()
		p["downloadId"] = "large"
		v, e := s.dashboardDownload("filesystem/download/"+method, jsonObject(p))
		if e != nil {
			t.Fatal(e)
		}
		return v
	}
	payload := []byte(strings.Repeat("导出", 200000))
	call("stage", map[string]any{"size": len(payload)})
	if _, e := s.dashboardDownload("filesystem/download/open", map[string]any{"downloadId": "large"}); e == nil {
		t.Fatal("incomplete export opened")
	}
	for offset := 0; offset < len(payload); offset += 192 * 1024 {
		end := minValue(offset+192*1024, len(payload))
		call("append", map[string]any{"offset": offset, "dataBase64": base64.StdEncoding.EncodeToString(payload[offset:end])})
	}
	call("open", map[string]any{})
	var out []byte
	for {
		row := call("read", map[string]any{}).(map[string]any)
		chunk, _ := base64.StdEncoding.DecodeString(row["dataBase64"].(string))
		out = append(out, chunk...)
		if row["done"] == true {
			break
		}
	}
	if string(out) != string(payload) {
		t.Fatal("staged bytes changed")
	}
	call("close", map[string]any{})
}
func TestDashboardHistoryAndMonitorIsolation(t *testing.T) {
	t.Setenv("DBX_PLUGIN_DATA_DIR", t.TempDir())
	a := &session{dashboard: newDashboardState("A")}
	b := &session{dashboard: newDashboardState("B")}
	ctx := context.Background()
	_, err := a.dashboardMetrics(ctx, nil, "/monitor/createOrUpdateConsumerMonitor.do", jsonObject(map[string]any{"consumeGroupName": "G", "minCount": 2, "maxDiffTotal": 99}))
	if err != nil {
		t.Fatal(err)
	}
	restored := &session{dashboard: newDashboardState("A")}
	got, err := restored.dashboardMetrics(ctx, nil, "/monitor/consumerMonitorConfigByGroupName.query", map[string]any{"consumeGroupName": "G"})
	if err != nil || floatValue(got.(map[string]any)["maxDiffTotal"]) != 99 {
		t.Fatalf("persistent config=%v %v", got, err)
	}
	got, err = b.dashboardMetrics(ctx, nil, "/dashboard/broker.query", map[string]any{"date": "2020-01-01"})
	if err != nil || len(got.(map[string]any)) != 0 {
		t.Fatal("history gap fabricated data")
	}
	if _, err = b.dashboardMetrics(ctx, nil, "/dashboard/broker.query", map[string]any{"date": "../../secret"}); err == nil {
		t.Fatal("unsafe date accepted")
	}
	a.startDashboardCollector()
	a.close()
	select {
	case <-a.dashboard.done:
	case <-time.After(time.Second):
		t.Fatal("collector did not stop")
	}
}
func TestTraceGraphOfficialShapeAndMissingPairs(t *testing.T) {
	fields := []string{"Pub", "1000", "region", "PG", "T", "id", "tag", "key", "127.0.0.1:10911", "5", "10", "0", "offset", "true"}
	records := strings.Join(fields, "\x01") + "\x02" + strings.Join([]string{"SubBefore", "1100", "region", "CG", "request", "id", "1", "key"}, "\x01") + "\x02"
	graph := buildTraceGraph([]*admin.MessageExt{{Body: []byte(records), BornHost: "client"}}, "id")
	producer := graph["producerNode"].(map[string]any)
	if producer["topic"] != "T" || producer["traceNode"].(map[string]any)["status"] != "success" {
		t.Fatal(producer)
	}
	node := graph["subscriptionNodeList"].([]any)[0].(map[string]any)["consumeNodeList"].([]map[string]any)[0]
	if node["status"] != "unknown" || node["endTimestamp"] != int64(-1) {
		t.Fatal("invented missing consume completion")
	}
}
func TestRunningInfoRepairPreservesStackAndObjectQueueKeys(t *testing.T) {
	raw := []byte("{\"mqTable\":{{\"topic\":\"T\",\"queueId\":0,\"brokerName\":\"B\"}:{\"cachedMsgCount\":2}}, \"jstack\":\"goroutine 1\nC:\\test\\main.go:3\n\" }")
	var data map[string]any
	if err := json.Unmarshal(repairRunningInfo(raw), &data); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(data["jstack"].(string), "C:\\test") {
		t.Fatal(data)
	}
}
func TestStats24HourFallbackMatchesUpstream(t *testing.T) {
	s := map[string]any{"statsDay": map[string]any{"sum": float64(0)}, "statsHour": map[string]any{"sum": float64(12)}, "statsMinute": map[string]any{"sum": float64(3)}}
	if stats24HourSum(s) != 12 {
		t.Fatal("hour fallback missing")
	}
}
