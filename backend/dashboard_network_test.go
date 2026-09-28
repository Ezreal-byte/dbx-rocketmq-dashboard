package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

// A transport fixture forwarding to the real isolated RocketMQ cluster.
func dashboardSocks(t *testing.T) (string, func() []string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var addresses []string
	var live sync.Map
	t.Cleanup(func() { listener.Close(); live.Range(func(k, v any) bool { k.(net.Conn).Close(); return true }) })
	go func() {
		for {
			conn, e := listener.Accept()
			if e != nil {
				return
			}
			live.Store(conn, true)
			go func() {
				defer conn.Close()
				defer live.Delete(conn)
				conn.SetDeadline(time.Now().Add(60 * time.Second))
				h := make([]byte, 2)
				if _, e := io.ReadFull(conn, h); e != nil {
					return
				}
				methods := make([]byte, int(h[1]))
				if _, e := io.ReadFull(conn, methods); e != nil {
					return
				}
				conn.Write([]byte{5, 0})
				head := make([]byte, 4)
				if _, e := io.ReadFull(conn, head); e != nil {
					return
				}
				var host string
				switch head[3] {
				case 1:
					ip := make([]byte, 4)
					if _, e := io.ReadFull(conn, ip); e != nil {
						return
					}
					host = net.IP(ip).String()
				case 3:
					n := make([]byte, 1)
					io.ReadFull(conn, n)
					b := make([]byte, int(n[0]))
					if _, e := io.ReadFull(conn, b); e != nil {
						return
					}
					host = string(b)
				case 4:
					ip := make([]byte, 16)
					if _, e := io.ReadFull(conn, ip); e != nil {
						return
					}
					host = net.IP(ip).String()
				default:
					return
				}
				port := make([]byte, 2)
				if _, e := io.ReadFull(conn, port); e != nil {
					return
				}
				address := net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(port))))
				mu.Lock()
				addresses = append(addresses, address)
				mu.Unlock()
				remote, e := net.DialTimeout("tcp", address, 3*time.Second)
				if e != nil {
					conn.Write([]byte{5, 5, 0, 1, 0, 0, 0, 0, 0, 0})
					return
				}
				defer remote.Close()
				conn.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 0})
				done := make(chan struct{})
				go func() { io.Copy(remote, conn); close(done) }()
				io.Copy(conn, remote)
				conn.Close()
				<-done
			}()
		}
	}()
	return listener.Addr().String(), func() []string { mu.Lock(); defer mu.Unlock(); return append([]string{}, addresses...) }
}

func TestDashboardRoutingIntegration(t *testing.T) {
	if os.Getenv("ROCKETMQ_INTEGRATION") != "1" {
		t.Skip("requires isolated broker")
	}
	t.Setenv("DBX_PLUGIN_DATA_DIR", t.TempDir())
	addr, targets := dashboardSocks(t)
	host, port, _ := net.SplitHostPort(addr)
	n, _ := strconv.Atoi(port)
	ns := os.Getenv("ROCKETMQ_NAMESRV_ADDR")
	for _, vip := range []bool{false, true} {
		t.Run(fmt.Sprintf("VIP=%t", vip), func(t *testing.T) {
			s, err := newSession(lifecycleParams{Connection: connection{ID: fmt.Sprint(vip), Config: externalConfig{NamesrvAddr: ns, VIPChannel: vip}}})
			if err != nil {
				t.Fatal(err)
			}
			s.connection.SocksProxy = &socksProxyConfig{Host: host, Port: n}
			defer s.close()
			if err = s.probe(); err != nil {
				t.Fatal(err)
			}
			dashboardCall(t, s, "/cluster/list.query", nil)
		})
	}
	seenNS, seenBroker, seenVIP := false, false, false
	_, nsPort, _ := net.SplitHostPort(ns)
	brokerPort := "30911"
	vipPort := "30909"
	if nsPort == "19876" {
		brokerPort = "20911"
		vipPort = "20909"
	}
	for _, target := range targets() {
		_, p, _ := net.SplitHostPort(target)
		seenNS = seenNS || p == nsPort
		seenBroker = seenBroker || p == brokerPort
		seenVIP = seenVIP || p == vipPort
	}
	if !seenNS || !seenBroker || !seenVIP {
		t.Fatalf("SOCKS bypass detected: %v", targets())
	}
	t.Log("PASS NameServer, discovered Broker and VIP connections all traversed SOCKS5")
	s, err := newSession(lifecycleParams{Connection: connection{ID: "failover", Config: externalConfig{NamesrvAddr: "127.0.0.1:1;" + ns}}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if err = s.probe(); err != nil {
		t.Fatal(err)
	}
	dashboardCall(t, s, "/cluster/list.query", nil)
	s.dashboard.queries["owned"] = dashboardQuery{Topic: "T", Rows: []any{}, Created: time.Now()}
	other, err := newSession(lifecycleParams{Connection: connection{ID: "other", Config: externalConfig{NamesrvAddr: ns}}})
	if err != nil {
		t.Fatal(err)
	}
	defer other.close()
	if err = other.probe(); err != nil {
		t.Fatal(err)
	}
	r := other.dashboardRequest(map[string]any{"path": "/message/queryMessagePageByTopic.query", "method": "POST", "body": map[string]any{"topic": "T", "taskId": "owned"}})
	if r["status"] == 0 {
		t.Fatal("cross-connection query task allowed")
	}
	s.startDashboardCollector()
	s.close()
	if _, _, err = s.requireClient(); err == nil {
		t.Fatal("closed connection remains usable")
	}
	if other.lifetime.Err() != nil {
		t.Fatal("closing one connection cancelled another")
	}
	t.Log("PASS NameServer failover, task isolation and disconnect lifecycle")
}

func TestDashboardTLSIntegration(t *testing.T) {
	if os.Getenv("ROCKETMQ_INTEGRATION") != "1" {
		t.Skip("requires isolated permissive TLS broker")
	}
	t.Setenv("DBX_PLUGIN_DATA_DIR", t.TempDir())
	s, err := newSession(lifecycleParams{Connection: connection{ID: "tls", Config: externalConfig{NamesrvAddr: os.Getenv("ROCKETMQ_NAMESRV_ADDR"), UseTLS: true, TLSSkipVerify: true}}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if err = s.probe(); err != nil {
		t.Fatal(err)
	}
	dashboardCall(t, s, "/cluster/list.query", nil)
}
