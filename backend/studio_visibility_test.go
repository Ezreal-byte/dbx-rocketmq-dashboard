package main

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"
)

// The upstream Topic page selects instances by name and hides rows whose
// instanceId differs. A successful API response alone did not catch issue #1.
func TestStudioTopicVisibilityIntegration(t *testing.T) {
	if os.Getenv("ROCKETMQ_INTEGRATION") != "1" {
		t.Skip("requires isolated RocketMQ")
	}
	t.Setenv("DBX_PLUGIN_DATA_DIR", t.TempDir())
	for _, tc := range []struct {
		label, name string
		socks       bool
	}{
		{"named", "本地 RocketMQ", false},
		{"socks", "SSH Docker MQ5 / 测试", true},
		{"cluster-fallback", "", false},
	} {
		t.Run(tc.label, func(t *testing.T) {
			s, err := newSession(lifecycleParams{Connection: connection{ID: tc.label, Name: tc.name, Config: externalConfig{NamesrvAddr: os.Getenv("ROCKETMQ_NAMESRV_ADDR")}}})
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			var destinations func() []string
			if tc.socks {
				addr, seen := dashboardSocks(t)
				destinations = seen
				host, port, _ := net.SplitHostPort(addr)
				n, _ := strconv.Atoi(port)
				s.connection.SocksProxy = &socksProxyConfig{Host: host, Port: n}
			}
			if err = s.probe(); err != nil {
				t.Fatal(err)
			}
			prefix := fmt.Sprintf("DBX_VISIBLE_%d_", time.Now().UnixNano())
			for i := 0; i < 5; i++ {
				name := fmt.Sprintf("%s%d", prefix, i)
				studioCall(t, s, "/topics/create", map[string]any{"name": name, "readQueues": 1, "writeQueues": 1, "perm": "RW"})
				defer studioCall(t, s, "/topics/delete", map[string]any{"name": name})
			}
			instances := studioCall(t, s, "/instances", nil).([]any)
			selected := instances[0].(map[string]any)["name"].(string)
			visible := 0
			for page := 1; page <= 3; page++ {
				result := studioCall(t, s, "/topics/page", map[string]any{"instanceId": selected, "search": prefix, "page": page, "pageSize": 2}).(map[string]any)
				if result["total"] != float64(5) {
					t.Fatalf("expected five Topics: %v", result)
				}
				for _, raw := range result["items"].([]any) {
					row := raw.(map[string]any)
					if row["instanceId"] != selected {
						t.Fatalf("Topic %s hidden by upstream UI: instanceId=%v selected=%s total=%v", row["name"], row["instanceId"], selected, result["total"])
					}
					visible++
				}
			}
			if visible != 5 {
				t.Fatalf("five Topics reported but only %d visible", visible)
			}
			capabilities := studioCall(t, s, "/instances/"+url.PathEscape(selected)+"/capabilities", nil).(map[string]any)
			if capabilities["instanceId"] != selected {
				t.Fatal("capability instance differs from selection")
			}
			if tc.socks {
				seenNS, seenBroker := false, false
				for _, addr := range destinations() {
					if addr == os.Getenv("ROCKETMQ_NAMESRV_ADDR") {
						seenNS = true
					} else {
						seenBroker = true
					}
				}
				if !seenNS || !seenBroker {
					t.Fatalf("NameServer/Broker bypassed SOCKS: %v", destinations())
				}
			}
		})
	}
}
