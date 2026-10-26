package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"strings"

	admin "github.com/amigoer/rocketmq-admin-go"
	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
)

func studioACLID(subject, resource string, policyType ...string) string {
	kind := "Custom"
	if len(policyType) > 0 {
		kind = policyType[0]
	}
	b, _ := json.Marshal([]string{subject, resource, kind})
	return base64.RawURLEncoding.EncodeToString(b)
}
func decodeStudioACLID(id string) (string, string, string, error) {
	b, e := base64.RawURLEncoding.DecodeString(id)
	if e != nil {
		return "", "", "", e
	}
	var v []string
	if e = json.Unmarshal(b, &v); e != nil || (len(v) != 2 && len(v) != 3) {
		return "", "", "", fmt.Errorf("invalid ACL rule identity")
	}
	if len(v) == 2 {
		v = append(v, "Custom")
	} // Existing 0.3.1 rule IDs.
	if !strings.HasPrefix(v[0], "User:") || strings.TrimPrefix(v[0], "User:") == "" || !validStudioACLResource(v[1]) || (v[2] != "Custom" && v[2] != "Default") {
		return "", "", "", fmt.Errorf("invalid ACL rule identity")
	}
	return v[0], v[1], v[2], nil
}

// Invalid resources parse as null in the Broker, which would delete the whole ACL.
func validStudioACLResource(resource string) bool {
	if resource == "*" {
		return true
	}
	parts := strings.SplitN(resource, ":", 2)
	if len(parts) != 2 || parts[1] == "" {
		return false
	}
	switch parts[0] {
	case "Topic", "Group", "Cluster":
	default:
		return false
	}
	return !strings.Contains(strings.TrimSuffix(parts[1], "*"), "*")
}

func studioACLResource(p map[string]any) (string, error) {
	resource := strings.TrimSpace(stringValue(p, "resource"))
	pattern := valueOrDefault(stringValue(p, "resourcePattern"), "LITERAL")
	if pattern != "PREFIX" && pattern != "LITERAL" && pattern != "ANY" {
		return "", fmt.Errorf("invalid resource pattern")
	}
	if resource == "" {
		return "", fmt.Errorf("resource is required")
	}
	if pattern == "ANY" && resource != "*" {
		return "", fmt.Errorf("ANY pattern requires resource *")
	}
	if pattern == "PREFIX" && !strings.HasSuffix(resource, "*") {
		resource += "*"
	}
	kind := valueOrDefault(stringValue(p, "resourceType"), "Any")
	if kind != "Any" {
		resource = kind + ":" + resource
	}
	if !validStudioACLResource(resource) {
		return "", fmt.Errorf("invalid ACL resource")
	}
	return resource, nil
}

func studioACLSourceIPs(value any) ([]string, error) {
	ips := []string{}
	var values []string
	switch v := value.(type) {
	case nil:
	case []string:
		values = v
	case []any:
		for _, item := range v {
			ip, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("sourceIps must be an array of IP addresses or CIDRs")
			}
			values = append(values, ip)
		}
	default:
		return nil, fmt.Errorf("sourceIps must be an array of IP addresses or CIDRs")
	}
	for _, ip := range values {
		ip = strings.TrimSpace(ip)
		if net.ParseIP(ip) == nil {
			if _, _, e := net.ParseCIDR(ip); e != nil {
				return nil, fmt.Errorf("invalid source IP or CIDR: %s", ip)
			}
		}
		ips = append(ips, ip)
	}
	return ips, nil
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
			rows = append(rows, map[string]any{"id": name, "username": name, "accessKey": name, "userStatus": v["userStatus"], "admin": strings.EqualFold(stringValue(v, "userType"), "Super"), "clusters": []string{s.clusterName}, "gmtCreate": nil})
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
		user := map[string]any{"username": name, "userType": kind}
		if path == "/acl/users/update" {
			if id := stringValue(p, "id"); id != "" && id != name {
				return nil, fmt.Errorf("changing a username requires creating a new user")
			}
			if _, present := p["admin"]; !present {
				delete(user, "userType")
			}
		}
		if path == "/acl/users/create" && stringValue(p, "secretKey", "password") == "" {
			return nil, fmt.Errorf("Secret Key is required when creating a user")
		}
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
	case "/acl/users/:id/credentials":
		name := stringValue(p, "id")
		brokers, e := s.dashboardBrokers(ctx, c, map[string]any{})
		if e != nil {
			return nil, e
		}
		var secret string
		first := true
		for _, broker := range sortedMapKeys(brokers) {
			r, err := invokeRemotingWithClient(ctx, brokers[broker], remoting.NewRequest(remoting.GetUser, map[string]string{"username": name}))
			if err != nil {
				return nil, fmt.Errorf("read ACL user on %s: %w", broker, err)
			}
			var user map[string]any
			if err = json.Unmarshal(r.Body, &user); err != nil {
				return nil, fmt.Errorf("invalid ACL user response on %s", broker)
			}
			if stringValue(user, "username") != name {
				return nil, fmt.Errorf("ACL user not found on %s", broker)
			}
			password := stringValue(user, "password")
			if !first && password != secret {
				return nil, fmt.Errorf("ACL user credentials differ between Brokers")
			}
			secret, first = password, false
		}
		return map[string]any{"id": name, "username": name, "accessKey": name, "secretKey": secret}, nil
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
				policyType := valueOrDefault(stringValue(policy, "policyType"), "Custom")
				for _, entry := range studioRows(policy["entries"]) {
					resource := stringValue(entry, "resource")
					parts := strings.SplitN(resource, ":", 2)
					kind := "Any"
					res := resource
					if len(parts) == 2 {
						kind, res = parts[0], parts[1]
					}
					pattern := "LITERAL"
					if res == "*" {
						pattern = "ANY"
					} else if strings.HasSuffix(res, "*") {
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
					actions := stringSlice(entry["actions"])
					for i := range actions {
						actions[i] = strings.ToUpper(actions[i])
					}
					ips := stringSlice(entry["sourceIps"])
					if ips == nil {
						ips = []string{}
					}
					rows = append(rows, map[string]any{"id": studioACLID(subject, resource, policyType), "policyType": policyType, "principal": principal, "resource": res, "resourceType": kind, "resourcePattern": pattern, "actions": actions, "sourceIps": ips, "decision": strings.ToUpper(stringValue(entry, "decision")), "scope": "cluster", "aclVersion": "2.0", "gmtCreate": nil})
				}
			}
		}
		return studioPage(rows, p), nil
	case "/acl/rules/delete":
		subject, resource, policyType, e := decodeStudioACLID(stringValue(p, "id"))
		if e != nil {
			return nil, e
		}
		return s.dashboardACL(ctx, c, "/acl/deleteAcl.do", map[string]any{"subject": subject, "resource": resource, "policyType": policyType})
	case "/acl/rules/create", "/acl/rules/update":
		subject := "User:" + strings.TrimPrefix(stringValue(p, "principal"), "User:")
		if subject == "User:" {
			return nil, fmt.Errorf("principal is required")
		}
		resource, e := studioACLResource(p)
		if e != nil {
			return nil, e
		}
		policyType := "Custom"
		if path == "/acl/rules/update" {
			oldSubject, oldResource, oldType, e := decodeStudioACLID(stringValue(p, "id"))
			if e != nil {
				return nil, e
			}
			if subject != oldSubject || resource != oldResource {
				return nil, fmt.Errorf("changing rule identity requires creating the new rule and deleting the old rule")
			}
			policyType = oldType
			// Old clients may omit the whitelist. Never silently remove a restriction.
			if value, present := p["sourceIps"]; !present || value == nil {
				return nil, fmt.Errorf("sourceIps is required when updating a rule; reload the rule before saving")
			}
		}
		actions := stringSlice(p["actions"])
		if len(actions) == 0 {
			return nil, fmt.Errorf("actions required")
		}
		for i, action := range actions {
			actions[i] = strings.ToUpper(action)
			switch actions[i] {
			case "PUB", "SUB", "ALL", "CREATE", "UPDATE", "DELETE", "GET", "LIST":
			default:
				return nil, fmt.Errorf("invalid ACL action: %s", action)
			}
		}
		decision := strings.ToUpper(valueOrDefault(stringValue(p, "decision"), "ALLOW"))
		if decision != "ALLOW" && decision != "DENY" {
			return nil, fmt.Errorf("invalid ACL decision")
		}
		ips, e := studioACLSourceIPs(p["sourceIps"])
		if e != nil {
			return nil, e
		}
		policy := map[string]any{"subject": subject, "policies": []any{map[string]any{"policyType": policyType, "entries": []any{map[string]any{"resource": resource, "actions": actions, "sourceIps": ips, "decision": decision}}}}}
		operation := "/acl/createAcl.do"
		if path == "/acl/rules/update" {
			operation = "/acl/updateAcl.do"
		}
		_, e = s.dashboardACL(ctx, c, operation, policy)
		if e != nil {
			return nil, e
		}
		row := jsonObject(p)
		row["id"] = studioACLID(subject, resource, policyType)
		row["policyType"] = policyType
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
