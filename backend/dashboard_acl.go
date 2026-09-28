package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	admin "github.com/amigoer/rocketmq-admin-go"
	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
)

// ACL 2.0 uses arrays and nested policy entries on the wire. The vendored
// convenience structs flatten policies, so use the actual protocol here.
func (s *session) dashboardACL(ctx context.Context, c *admin.Client, path string, p map[string]any) (any, error) {
	brokers, err := s.dashboardBrokers(ctx, c, p)
	if err != nil {
		return nil, err
	}
	if path == "/acl/users.query" || path == "/acl/acls.query" {
		common := map[string]any{}
		first := true
		for _, name := range sortedMapKeys(brokers) {
			code := remoting.ListUser
			if path == "/acl/acls.query" {
				code = remoting.ListAcl
			}
			response, e := invokeRemotingWithClient(ctx, brokers[name], remoting.NewRequest(code, map[string]string{"filter": "", "subjectFilter": "", "resourceFilter": ""}))
			if e != nil {
				return nil, fmt.Errorf("ACL 2.0 query on %s: %w", name, e)
			}
			var rows []map[string]any
			// Broker SUCCESS with no body represents an empty ACL/user list.
			if len(response.Body) > 0 {
				e = json.Unmarshal(response.Body, &rows)
			}
			if e != nil {
				return nil, fmt.Errorf("invalid ACL response: %w", e)
			}
			current := map[string]any{}
			for _, row := range rows {
				if path == "/acl/users.query" {
					delete(row, "password")
				}
				b, _ := json.Marshal(row)
				key := string(b)
				search := stringValue(p, "searchParam")
				if search != "" && !strings.Contains(key, search) {
					continue
				}
				current[key] = row
			}
			if first {
				common = current
				first = false
			} else {
				for key := range common {
					if _, ok := current[key]; !ok {
						delete(common, key)
					}
				}
			}
		}
		result := []any{}
		for _, key := range sortedKeys(common) {
			result = append(result, common[key])
		}
		return result, nil
	}
	fields := map[string]string{}
	var body any
	code := 0
	switch path {
	case "/acl/createUser.do", "/acl/updateUser.do":
		user := nestedMap(p, "userInfo")
		if stringValue(user, "username") == "" {
			return nil, fmt.Errorf("username is required")
		}
		body = user
		fields["username"] = stringValue(user, "username")
		code = remoting.CreateUser
		if path == "/acl/updateUser.do" {
			code = remoting.UpdateUser
		}
	case "/acl/deleteUser.do":
		code = remoting.DeleteUser
		fields["username"] = stringValue(p, "username")
		if fields["username"] == "" {
			return nil, fmt.Errorf("username is required")
		}
	case "/acl/deleteAcl.do":
		code = remoting.DeleteAcl
		fields["subject"] = stringValue(p, "subject")
		fields["resource"] = stringValue(p, "resource")
		if fields["subject"] == "" {
			return nil, fmt.Errorf("subject is required")
		}
	case "/acl/createAcl.do", "/acl/updateAcl.do":
		code = remoting.CreateAcl
		if path == "/acl/updateAcl.do" {
			code = remoting.UpdateAcl
		}
		subject := stringValue(p, "subject")
		if subject == "" {
			return nil, fmt.Errorf("subject is required")
		}
		policies := []any{}
		rawPolicies, _ := p["policies"].([]any)
		for _, raw := range rawPolicies {
			policy, ok := raw.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("invalid policy")
			}
			entries := []any{}
			rawEntries, _ := policy["entries"].([]any)
			for _, rawEntry := range rawEntries {
				entry, ok := rawEntry.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("invalid entry")
				}
				resources := stringSlice(entry["resource"])
				if str, ok := entry["resource"].(string); ok {
					resources = []string{str}
				}
				for _, resource := range resources {
					row := map[string]any{}
					for k, v := range entry {
						row[k] = v
					}
					row["resource"] = resource
					entries = append(entries, row)
				}
			}
			policies = append(policies, map[string]any{"policyType": policy["policyType"], "entries": entries})
		}
		if len(policies) == 0 {
			return nil, fmt.Errorf("policies are required")
		}
		body = map[string]any{"subject": subject, "policies": policies}
		fields["subject"] = subject
	default:
		return nil, fmt.Errorf("unknown ACL operation %s", path)
	}
	return mutateBrokers(brokers, func(addr string) error {
		cmd := remoting.NewRequest(code, fields)
		if body != nil {
			cmd.Body, _ = json.Marshal(body)
		}
		_, e := invokeRemotingWithClient(ctx, addr, cmd)
		return e
	})
}
