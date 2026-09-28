package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	admin "github.com/amigoer/rocketmq-admin-go"
	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
	"strings"
)

func studioACLID(subject, resource string) string {
	b, _ := json.Marshal([]string{subject, resource})
	return base64.RawURLEncoding.EncodeToString(b)
}
func decodeStudioACLID(id string) (string, string, error) {
	b, e := base64.RawURLEncoding.DecodeString(id)
	if e != nil {
		return "", "", e
	}
	var v []string
	if e = json.Unmarshal(b, &v); e != nil || len(v) != 2 {
		return "", "", fmt.Errorf("invalid ACL rule identity")
	}
	return v[0], v[1], nil
}
func (s *session) studioACL(ctx context.Context, c *admin.Client, path string, p map[string]any) (any, error) {
	switch path {
	case "/acl/users", "/acl/users/page":
		raw, e := s.dashboardACL(ctx, c, "/acl/users.query", map[string]any{"searchParam": p["keyword"]})
		if e != nil {
			return nil, e
		}
		rows := []any{}
		for _, v := range studioRows(raw) {
			name := stringValue(v, "username")
			rows = append(rows, map[string]any{"id": name, "username": name, "accessKey": name, "admin": strings.EqualFold(stringValue(v, "userType"), "Super"), "clusters": []string{s.clusterName}, "gmtCreate": nil})
		}
		if strings.HasSuffix(path, "/page") {
			return studioPage(rows, p), nil
		}
		return rows, nil
	case "/acl/users/create", "/acl/users/update":
		name := stringValue(p, "username")
		kind := "Normal"
		if boolValue(p, false, "admin") {
			kind = "Super"
		}
		user := map[string]any{"username": name, "userType": kind, "userStatus": "Enable"}
		if secret := stringValue(p, "secretKey", "password"); secret != "" {
			user["password"] = secret
		}
		operation := "/acl/createUser.do"
		if path == "/acl/users/update" {
			operation = "/acl/updateUser.do"
		}
		_, e := s.dashboardACL(ctx, c, operation, map[string]any{"userInfo": user})
		if e != nil {
			return nil, e
		}
		return map[string]any{"id": name, "username": name, "admin": kind == "Super", "clusters": []string{s.clusterName}}, nil
	case "/acl/users/delete":
		return s.dashboardACL(ctx, c, "/acl/deleteUser.do", map[string]any{"username": stringValue(p, "id")})
	case "/acl/rules":
		raw, e := s.dashboardACL(ctx, c, "/acl/acls.query", map[string]any{})
		if e != nil {
			return nil, e
		}
		rows := []any{}
		for _, acl := range studioRows(raw) {
			subject := stringValue(acl, "subject")
			for _, policy := range studioRows(acl["policies"]) {
				for _, entry := range studioRows(policy["entries"]) {
					resource := stringValue(entry, "resource")
					parts := strings.SplitN(resource, ":", 2)
					kind := "Any"
					res := resource
					if len(parts) == 2 {
						kind, res = parts[0], parts[1]
					}
					pattern := "LITERAL"
					if strings.HasSuffix(res, "*") {
						pattern = "PREFIX"
						res = strings.TrimSuffix(res, "*")
					}
					principal := strings.TrimPrefix(subject, "User:")
					if q := stringValue(p, "principal"); q != "" && q != principal {
						continue
					}
					if q := stringValue(p, "resource"); q != "" && !strings.Contains(res, q) {
						continue
					}
					if q := stringValue(p, "decision"); q != "" && !strings.EqualFold(q, stringValue(entry, "decision")) {
						continue
					}
					rows = append(rows, map[string]any{"id": studioACLID(subject, resource), "principal": principal, "resource": res, "resourceType": kind, "resourcePattern": pattern, "actions": entry["actions"], "decision": entry["decision"], "scope": "cluster", "aclVersion": "2.0", "gmtCreate": nil})
				}
			}
		}
		return studioPage(rows, p), nil
	case "/acl/rules/delete":
		subject, resource, e := decodeStudioACLID(stringValue(p, "id"))
		if e != nil {
			return nil, e
		}
		return s.dashboardACL(ctx, c, "/acl/deleteAcl.do", map[string]any{"subject": subject, "resource": resource})
	case "/acl/rules/create", "/acl/rules/update":
		subject := "User:" + strings.TrimPrefix(stringValue(p, "principal"), "User:")
		resource := stringValue(p, "resource")
		if resource == "" {
			return nil, fmt.Errorf("resource is required")
		}
		if stringValue(p, "resourcePattern") == "PREFIX" {
			resource += "*"
		}
		if kind := stringValue(p, "resourceType"); kind != "" && kind != "Any" {
			resource = kind + ":" + resource
		}
		if path == "/acl/rules/update" {
			oldSubject, oldResource, e := decodeStudioACLID(stringValue(p, "id"))
			if e != nil {
				return nil, e
			}
			if subject != oldSubject || resource != oldResource {
				return nil, fmt.Errorf("changing rule identity requires creating the new rule and deleting the old rule")
			}
		}
		actions := stringSlice(p["actions"])
		if len(actions) == 0 {
			return nil, fmt.Errorf("actions required")
		}
		policy := map[string]any{"subject": subject, "policies": []any{map[string]any{"policyType": "Custom", "entries": []any{map[string]any{"resource": resource, "actions": actions, "sourceIps": []string{}, "decision": valueOrDefault(stringValue(p, "decision"), "Allow")}}}}}
		operation := "/acl/createAcl.do"
		if path == "/acl/rules/update" {
			operation = "/acl/updateAcl.do"
		}
		_, e := s.dashboardACL(ctx, c, operation, policy)
		if e != nil {
			return nil, e
		}
		row := jsonObject(p)
		row["id"] = studioACLID(subject, resource)
		row["aclVersion"] = "2.0"
		return row, nil
	case "/acl/cluster-config":
		cluster := stringValue(p, "clusterId")
		if cluster != "" && cluster != s.clusterName {
			return nil, fmt.Errorf("unknown cluster %s", cluster)
		}
		brokers, e := s.dashboardBrokers(ctx, c, map[string]any{})
		if e != nil {
			return nil, e
		}
		accounts := []any{}
		white := map[string]bool{}
		seen := map[string]bool{}
		for _, name := range sortedMapKeys(brokers) {
			r, e := invokeRemotingWithClient(ctx, brokers[name], remoting.NewRequest(remoting.GetBrokerClusterAclInfo, nil))
			if e != nil {
				return nil, e
			}
			var body map[string]any
			if e = json.Unmarshal(r.Body, &body); e != nil {
				return nil, e
			}
			for _, a := range studioRows(body["accounts"]) {
				delete(a, "secretKey")
				key := stringValue(a, "accessKey")
				if !seen[key] {
					seen[key] = true
					accounts = append(accounts, a)
				}
			}
			for _, a := range stringSlice(body["globalWhiteRemoteAddresses"]) {
				white[a] = true
			}
		}
		return map[string]any{"clusterId": s.clusterName, "aclEnabled": s.aclEnabled, "aclVersion": "1.0", "globalWhiteRemoteAddresses": sortedKeys(white), "accounts": accounts, "accountCount": len(accounts)}, nil
	case "/acl/plain-access-config":
		if stringValue(p, "accessKey") == "" {
			return nil, fmt.Errorf("accessKey required")
		}
		brokers, e := s.dashboardBrokers(ctx, c, map[string]any{})
		if e != nil {
			return nil, e
		}
		fields := map[string]string{}
		for _, key := range []string{"accessKey", "secretKey", "whiteRemoteAddress", "defaultTopicPerm", "defaultGroupPerm"} {
			if v, ok := p[key]; ok {
				fields[key] = fmt.Sprint(v)
			}
		}
		fields["admin"] = fmt.Sprint(boolValue(p, false, "admin"))
		for _, key := range []string{"topicPerms", "groupPerms"} {
			fields[key] = strings.Join(stringSlice(p[key]), ",")
		}
		_, e = mutateBrokers(brokers, func(addr string) error {
			_, e := invokeRemotingWithClient(ctx, addr, remoting.NewRequest(remoting.UpdateAndCreateAclConfig, fields))
			return e
		})
		if e != nil {
			return nil, e
		}
		result := jsonObject(p)
		delete(result, "secretKey")
		return result, nil
	}
	return nil, fmt.Errorf("unhandled Studio ACL endpoint %s", path)
}
