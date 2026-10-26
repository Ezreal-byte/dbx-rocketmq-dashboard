package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
)

func TestStudioACLIdentity(t *testing.T) {
	for _, resource := range []string{"*", "Topic:*", "Group:*", "Topic:prefix*", "Group:literal"} {
		for _, policy := range []string{"Custom", "Default"} {
			subject, got, kind, err := decodeStudioACLID(studioACLID("User:test", resource, policy))
			if err != nil || subject != "User:test" || got != resource || kind != policy {
				t.Fatalf("identity: %s %s %v", resource, policy, err)
			}
		}
	}
	if studioACLID("User:test", "Topic:*", "Custom") == studioACLID("User:test", "Topic:*", "Default") {
		t.Fatal("policy IDs collide")
	}
	for _, resource := range []string{"", "Topic:", "unknown:topic", "Topic:**", "Topic:foo*bar", "foo"} {
		if _, _, _, err := decodeStudioACLID(studioACLID("User:test", resource)); err == nil {
			t.Fatalf("unsafe resource accepted: %q", resource)
		}
	}
	legacy, _ := json.Marshal([]string{"User:test", "Topic:*"})
	_, _, kind, err := decodeStudioACLID(base64.RawURLEncoding.EncodeToString(legacy))
	if err != nil || kind != "Custom" {
		t.Fatal("0.3.1 IDs no longer supported")
	}
	for _, tc := range []struct{ resource, pattern, want string }{
		{"*", "PREFIX", "Topic:*"}, {"*", "ANY", "Topic:*"}, {"prefix", "PREFIX", "Topic:prefix*"}, {"prefix*", "PREFIX", "Topic:prefix*"}, {"literal", "LITERAL", "Topic:literal"},
	} {
		got, err := studioACLResource(map[string]any{"resource": tc.resource, "resourceType": "Topic", "resourcePattern": tc.pattern})
		if err != nil || got != tc.want {
			t.Fatalf("resource: %v => %s %v", tc, got, err)
		}
	}
	if _, err := studioACLSourceIPs([]string{"not-an-ip"}); err == nil {
		t.Fatal("invalid whitelist accepted")
	}
	if _, err := studioACLSourceIPs([]string{"127.0.0.1", "10.0.0.0/8", "::1", "2001:db8::/32"}); err != nil {
		t.Fatal(err)
	}
}

// Exercise the actual Studio contract and Broker readback, including the user's
// Topic:* / Group:* fixture. Run both directly and through the DBX SOCKS route.
func TestStudioACLIntegration(t *testing.T) {
	if os.Getenv("ROCKETMQ_INTEGRATION") != "1" || !strings.HasPrefix(os.Getenv("ROCKETMQ_VERSION"), "5.") {
		t.Skip("isolated RocketMQ 5.x required")
	}
	t.Setenv("DBX_PLUGIN_DATA_DIR", t.TempDir())
	for _, socks := range []bool{false, true} {
		t.Run(fmt.Sprintf("socks=%v", socks), func(t *testing.T) {
			s, err := newSession(lifecycleParams{Connection: connection{ID: "acl-test", Name: "ACL regression", Config: externalConfig{NamesrvAddr: os.Getenv("ROCKETMQ_NAMESRV_ADDR")}}})
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			var destinations func() []string
			if socks {
				addr, seen := dashboardSocks(t)
				destinations = seen
				host, port, _ := net.SplitHostPort(addr)
				n, _ := strconv.Atoi(port)
				s.connection.SocksProxy = &socksProxyConfig{Host: host, Port: n}
			}
			if err = s.probe(); err != nil {
				t.Fatal(err)
			}
			client, _, _ := s.requireClient()
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			username := fmt.Sprintf("DBX_ACL_%d", time.Now().UnixNano())
			secret := "isolated-acl-regression-only"
			studioCall(t, s, "/acl/users/create", map[string]any{"username": username, "secretKey": secret, "admin": false})
			defer studioCall(t, s, "/acl/users/delete", map[string]any{"id": username})
			subject := "User:" + username
			seed := func(policy, resource string) {
				_, e := s.dashboardACL(ctx, client, "/acl/createAcl.do", map[string]any{"subject": subject, "policies": []any{map[string]any{"policyType": policy, "entries": []any{map[string]any{"resource": resource, "actions": []string{"Pub", "Sub", "All"}, "decision": "Allow", "sourceIps": []string{"127.0.0.1/32"}}}}}})
				if e != nil {
					t.Fatal(e)
				}
			}
			for _, resource := range []string{"Topic:*", "Group:*", "*", "Topic:prefix*"} {
				seed("Custom", resource)
			}
			seed("Default", "Topic:*")
			list := func() []map[string]any {
				page := studioCall(t, s, "/acl/rules", map[string]any{"principal": username, "pageSize": 100}).(map[string]any)
				return studioRows(page["items"])
			}
			rows := list()
			if len(rows) != 5 {
				t.Fatalf("missing policies: %v", rows)
			}
			ids := map[string]bool{}
			for _, row := range rows {
				id := stringValue(row, "id")
				if ids[id] {
					t.Fatal("policy IDs collide")
				}
				ids[id] = true
				if row["resource"] == "" || row["decision"] != "ALLOW" || !reflect.DeepEqual(stringSlice(row["actions"]), []string{"PUB", "SUB", "ALL"}) || !reflect.DeepEqual(stringSlice(row["sourceIps"]), []string{"127.0.0.1/32"}) {
					t.Fatalf("incorrect Studio contract: %v", row)
				}
				if row["resource"] == "*" && row["resourcePattern"] != "ANY" {
					t.Fatalf("wildcard lost: %v", row)
				}
				row["sourceIps"] = []string{"10.0.0.0/8", "192.168.1.2"}
				row["actions"] = []string{"PUB", "SUB", "GET"}
				row["decision"] = "DENY"
				studioCall(t, s, "/acl/rules/update", row)
			}
			for _, row := range list() {
				if row["decision"] != "DENY" || !reflect.DeepEqual(stringSlice(row["sourceIps"]), []string{"10.0.0.0/8", "192.168.1.2"}) || !reflect.DeepEqual(stringSlice(row["actions"]), []string{"PUB", "SUB", "GET"}) {
					t.Fatalf("update not persisted: %v", row)
				}
			}
			// Delete Default and Custom entries independently; preserve every sibling.
			for i, row := range rows {
				studioCall(t, s, "/acl/rules/delete", map[string]any{"id": row["id"]})
				remaining := list()
				if len(remaining) != len(rows)-i-1 {
					t.Fatalf("delete removed wrong entries: %v", remaining)
				}
				for _, other := range remaining {
					if other["id"] == row["id"] {
						t.Fatal("deleted rule remains")
					}
				}
			}
			// The form can create a wildcard even when the legacy PREFIX control is selected.
			created := studioCall(t, s, "/acl/rules/create", map[string]any{"principal": username, "resource": "*", "resourceType": "Topic", "resourcePattern": "PREFIX", "actions": []string{"PUB"}, "decision": "ALLOW", "sourceIps": []string{"127.0.0.1"}}).(map[string]any)
			row := list()[0]
			if row["resource"] != "*" || row["resourcePattern"] != "ANY" {
				t.Fatal(row)
			}
			delete(row, "sourceIps")
			r := s.studioRequest(map[string]any{"method": "POST", "path": "/acl/rules/update", "body": row})
			if r["code"] == 0 {
				t.Fatal("omitted whitelist silently erased restriction")
			}
			if !reflect.DeepEqual(stringSlice(list()[0]["sourceIps"]), []string{"127.0.0.1"}) {
				t.Fatal("whitelist changed after rejected update")
			}
			for _, invalid := range []any{nil, "10.0.0.0/8", []any{123}, []string{"invalid-ip"}} {
				row["sourceIps"] = invalid
				r = s.studioRequest(map[string]any{"method": "POST", "path": "/acl/rules/update", "body": jsonObject(row)})
				if r["code"] == 0 {
					t.Fatalf("invalid whitelist accepted: %v", invalid)
				}
			}
			row["sourceIps"] = []string{}
			studioCall(t, s, "/acl/rules/update", row)
			if len(stringSlice(list()[0]["sourceIps"])) != 0 {
				t.Fatal("explicit whitelist clear failed")
			}
			studioCall(t, s, "/acl/rules/delete", map[string]any{"id": created["id"]})
			// Editing privileges must not reset a disabled user's status or password.
			if _, err = s.dashboardACL(ctx, client, "/acl/updateUser.do", map[string]any{"userInfo": map[string]any{"username": username, "userStatus": "Disable"}}); err != nil {
				t.Fatal(err)
			}
			studioCall(t, s, "/acl/users/update", map[string]any{"id": username, "username": username, "admin": true})
			brokers, err := s.dashboardBrokers(ctx, client, nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, addr := range brokers {
				r, e := invokeRemotingWithClient(ctx, addr, remoting.NewRequest(remoting.GetUser, map[string]string{"username": username}))
				if e != nil {
					t.Fatal(e)
				}
				var user map[string]any
				if e = json.Unmarshal(r.Body, &user); e != nil {
					t.Fatal(e)
				}
				if !strings.EqualFold(stringValue(user, "userStatus"), "Disable") || user["password"] != secret || user["userType"] != "Super" {
					t.Fatalf("user update status=%v type=%v passwordPresent=%v passwordMatches=%v", user["userStatus"], user["userType"], user["password"] != nil, user["password"] == secret)
				}
			}
			credentials := studioCall(t, s, "/acl/users/"+username+"/credentials", nil).(map[string]any)
			if credentials["secretKey"] != secret {
				t.Fatal("explicit credential retrieval failed")
			}
			studioCall(t, s, "/acl/users/update", map[string]any{"username": username, "secretKey": secret + "-rotated"})
			credentials = studioCall(t, s, "/acl/users/"+username+"/credentials", nil).(map[string]any)
			if credentials["secretKey"] != secret+"-rotated" {
				t.Fatal("password rotation failed")
			}
			users := studioCall(t, s, "/acl/users", map[string]any{"keyword": username})
			userRows := studioRows(users)
			if len(userRows) != 1 || userRows[0]["admin"] != true || !strings.EqualFold(stringValue(userRows[0], "userStatus"), "Disable") {
				t.Fatal("password rotation changed status or admin permissions")
			}
			encoded, _ := json.Marshal(users)
			if strings.Contains(string(encoded), secret) {
				t.Fatal("password leaked in user list")
			}
			if socks {
				seenNS, seenBroker := false, false
				for _, addr := range destinations() {
					if addr == os.Getenv("ROCKETMQ_NAMESRV_ADDR") {
						seenNS = true
					} else {
						seenBroker = true
					}
				}
				if !seenNS || !seenBroker {
					t.Fatalf("ACL discovery bypassed SOCKS: %v", destinations())
				}
			}
		})
	}
}
