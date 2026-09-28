package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

func TestRemotingDisconnectWakesPendingRequests(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, e := listener.Accept()
		if e == nil {
			defer conn.Close()
			readRemotingFrame(conn)
		}
	}()
	client := remoting.NewClient(listener.Addr().String(), time.Second)
	if err = client.Connect(); err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	_, err = client.InvokeSync(ctx, remoting.NewRequest(105, nil))
	if !errors.Is(err, remoting.ErrConnectionClosed) || time.Since(start) > time.Second {
		t.Fatalf("disconnect not propagated promptly: %v", err)
	}
	if err = client.Connect(); !errors.Is(err, remoting.ErrConnectionClosed) {
		t.Fatal("closed transport was reused")
	}
}

func TestDashboardAuthenticationIntegration(t *testing.T) {
	ns := os.Getenv("ROCKETMQ_AUTH_NAMESRV")
	if ns == "" {
		t.Skip("requires isolated ACL-enabled broker")
	}
	t.Setenv("DBX_PLUGIN_DATA_DIR", t.TempDir())
	for _, tc := range []struct {
		name, key, secret string
		allow             bool
	}{
		{"admin", "dbx-test-admin", "dbx-test-admin-secret", true},
		{"wrong-secret", "dbx-test-admin", "incorrect", false},
		{"missing", "", "", false},
		{"limited", "dbx-test-limited", "dbx-test-limited-secret", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, e := newSession(lifecycleParams{Connection: connection{ID: tc.name, Config: externalConfig{NamesrvAddr: ns}, ConnectionSecrets: map[string]string{"access_key": tc.key, "secret_key": tc.secret}}})
			if e != nil {
				t.Fatal(e)
			}
			defer s.close()
			e = s.probe()
			if !tc.allow {
				if tc.name == "limited" {
					if e != nil {
						t.Fatal(e)
					}
					r := s.dashboardRequest(map[string]any{"path": "/topic/createOrUpdate.do", "method": "POST", "body": jsonObject(map[string]any{"topicName": "DENIED_WRITE", "brokerNameList": []string{"dbx-dashboard-auth"}, "readQueueNums": 1, "writeQueueNums": 1})})
					if r["status"] == 0 {
						t.Fatal("unauthorized mutation succeeded")
					}
					e = fmt.Errorf("%v", r["errMsg"])
				}
				if e == nil {
					t.Fatal("invalid credentials accepted by connection test")
				}
				if !strings.Contains(strings.ToLower(e.Error()), "acl") && !strings.Contains(strings.ToLower(e.Error()), "access") && !strings.Contains(strings.ToLower(e.Error()), "permission") && !strings.Contains(strings.ToLower(e.Error()), "signature") {
					t.Fatalf("expected authentication/permission error, got: %v", e)
				}
				if strings.Contains(e.Error(), tc.secret) && tc.secret != "" {
					t.Fatal("error disclosed secret")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			dashboardCall(t, s, "/cluster/list.query", nil)
			topic := fmt.Sprintf("DBX_AUTH_%d", time.Now().UnixNano())
			dashboardCall(t, s, "/topic/createOrUpdate.do", map[string]any{"topicName": topic, "brokerNameList": []string{"dbx-dashboard-auth"}, "readQueueNums": 1, "writeQueueNums": 1})
			defer dashboardCall(t, s, "/topic/deleteTopic.do", map[string]any{"topic": topic})
			dashboardCall(t, s, "/topic/sendTopicMessage.do", map[string]any{"topic": topic, "messageBody": "authenticated"})
		})
	}
}

func TestDashboardRealPartialFailure(t *testing.T) {
	if os.Getenv("ROCKETMQ_AUTH_NAMESRV") == "" {
		t.Skip("requires isolated normal and ACL brokers")
	}
	t.Setenv("DBX_PLUGIN_DATA_DIR", t.TempDir())
	s, e := newSession(lifecycleParams{Connection: connection{ID: "partial", Config: externalConfig{NamesrvAddr: "127.0.0.1:29876"}}})
	if e != nil {
		t.Fatal(e)
	}
	defer s.close()
	if e = s.probe(); e != nil {
		t.Fatal(e)
	}
	c, _, _ := s.requireClient()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	good, e := s.dashboardBrokers(ctx, c, map[string]any{})
	if e != nil {
		t.Fatal(e)
	}
	first := good[sortedMapKeys(good)[0]]
	denied, e := s.proxies.ProxyFor("127.0.0.1:40911", proxyTargetBroker)
	if e != nil {
		t.Fatal(e)
	}
	topic := fmt.Sprintf("DBX_PARTIAL_%d", time.Now().UnixNano())
	defer dashboardCall(t, s, "/topic/deleteTopic.do", map[string]any{"topic": topic})
	_, e = mutateBrokers(map[string]string{"available": first, "denied": denied}, func(addr string) error {
		return writeTopicConfig(ctx, addr, &topicConfigWire{TopicName: topic, ReadQueueNums: 1, WriteQueueNums: 1, Perm: 6})
	})
	if e == nil || !strings.Contains(e.Error(), "available") || !strings.Contains(e.Error(), "denied") {
		t.Fatalf("partial failure not explicit: %v", e)
	}
	configs, e := fetchAllTopicConfigs(ctx, first)
	if e != nil {
		t.Fatal(e)
	}
	if configs[topic] == nil {
		t.Fatal("successful broker did not apply write")
	}
}
