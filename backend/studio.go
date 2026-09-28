package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

)

// Studio is an API compatibility layer over the same per-connection Go client.
// Only registered routes are reachable, and writes fail closed on read-only sessions.
var studioRoutes = map[string]dashboardRoute{
	"/settings/datasources": {"GET", false},
	"/instances": {"GET", false}, "/instances/:id/capabilities": {"GET", false},
	"/dashboard": {"GET", false}, "/clusters": {"GET", false}, "/clusters/registry": {"GET", false}, "/clusters/:id": {"GET", false},
	"/clusters/:id/broker-config-diff": {"GET", false}, "/clusters/config/preview": {"POST", false}, "/clusters/config/update": {"POST", true},
	"/nameservers": {"GET", false}, "/nameservers/config-diff": {"GET", false},
	"/topics": {"GET", false}, "/topics/page": {"GET", false}, "/topics/export": {"GET", false},
	"/topics/create": {"POST", true}, "/topics/update": {"POST", true}, "/topics/delete": {"POST", true}, "/topics/import": {"POST", true}, "/topics/send": {"POST", true},
	"/topics/:id/routes": {"GET", false}, "/topics/:id/consumers": {"GET", false}, "/topics/:id/consumers/page": {"GET", false},
	"/groups": {"GET", false}, "/groups/page": {"GET", false}, "/groups/export": {"GET", false},
	"/groups/create": {"POST", true}, "/groups/settings": {"POST", true}, "/groups/delete": {"POST", true}, "/groups/import": {"POST", true},
	"/groups/reset-offset": {"POST", true}, "/groups/reset-offset/preview": {"POST", false},
	"/groups/:id": {"GET", false}, "/groups/:id/refresh": {"GET", false}, "/groups/:id/progress": {"GET", false}, "/groups/:id/subscriptions": {"GET", false}, "/groups/:id/settings": {"GET", false}, "/groups/:id/instances/:client/stack": {"GET", false},
	"/messages": {"GET", false}, "/messages/page": {"GET", false}, "/messages/queues": {"GET", false}, "/messages/queue-message": {"GET", false}, "/messages/direct-consume": {"POST", true}, "/messages/:id/trace": {"GET", false}, "/messages/trace-by-key": {"GET", false},
	"/dlq": {"GET", false}, "/dlq/:id/messages": {"GET", false}, "/dlq/resend": {"POST", true}, "/dlq/resend-selected": {"POST", true}, "/dlq/export": {"GET", false}, "/dlq/export-excel": {"GET", false},
	"/acl/users": {"GET", false}, "/acl/users/page": {"GET", false}, "/acl/users/create": {"POST", true}, "/acl/users/update": {"POST", true}, "/acl/users/delete": {"POST", true},
	"/acl/rules": {"GET", false}, "/acl/rules/create": {"POST", true}, "/acl/rules/update": {"POST", true}, "/acl/rules/delete": {"POST", true},
	"/acl/cluster-config": {"GET", false}, "/acl/plain-access-config": {"POST", true},
	"/clients": {"GET", false}, "/producer/groups": {"GET", false}, "/producer/connection": {"GET", false},
	"/metrics/profiles": {"GET", false}, "/metrics/query": {"POST", false},
}

func studioRoute(path string) (string, map[string]any) {
	if _, ok := studioRoutes[path]; ok {
		return path, map[string]any{}
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for _, pattern := range sortedKeys(studioRoutes) {
		patternParts := strings.Split(strings.Trim(pattern, "/"), "/")
		if len(parts) != len(patternParts) {
			continue
		}
		params := map[string]any{}
		matched := true
		for i, p := range patternParts {
			if strings.HasPrefix(p, ":") {
				v, e := url.PathUnescape(parts[i])
				if e != nil || v == "" {
					matched = false
					break
				}
				params[p[1:]] = v
			} else if p != parts[i] {
				matched = false
				break
			}
		}
		if matched {
			return pattern, params
		}
	}
	return "", nil
}
func studioResult(data any, err error) map[string]any {
	if err != nil {
		return map[string]any{"code": -1, "message": err.Error(), "data": data}
	}
	return map[string]any{"code": 0, "message": "", "data": data}
}
func (s *session) studioRequest(request map[string]any) map[string]any {
	path, segments := studioRoute(stringValue(request, "path"))
	route, ok := studioRoutes[path]
	if !ok || strings.ToUpper(stringValue(request, "method")) != route.Method {
		return studioResult(nil, fmt.Errorf("unknown Studio endpoint or method: %s", stringValue(request, "path")))
	}
	s.operations.RLock()
	defer s.operations.RUnlock()
	if route.Write {
		if e := s.writeAllowed(); e != nil {
			return studioResult(nil, e)
		}
	}
	p := map[string]any{}
	for k, v := range nestedMap(request, "query") {
		p[k] = v
	}
	for k, v := range nestedMap(request, "body") {
		p[k] = v
	}
	for k, v := range segments {
		p[k] = v
	}
	// A Studio instance represents the active DBX connection only; credentials never enter the UI.
	if id := stringValue(p, "instanceId"); id != "" && id != "1" && id != valueOrDefault(s.name, s.clusterName) {
		return studioResult(nil, fmt.Errorf("instance does not belong to this DBX connection"))
	}
	c, _, err := s.requireClient()
	if err != nil {
		return studioResult(nil, err)
	}
	parent := s.lifetime
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, 55*time.Second)
	defer cancel()
	var data any
	switch {
	case strings.HasPrefix(path, "/topics") || strings.HasPrefix(path, "/groups"):
		data, err = s.studioMetadata(ctx, c, path, p)
	case strings.HasPrefix(path, "/messages") || strings.HasPrefix(path, "/dlq"):
		data, err = s.studioMessages(ctx, c, path, p)
	case strings.HasPrefix(path, "/acl/"):
		data, err = s.studioACL(ctx, c, path, p)
	default:
		data, err = s.studioTopology(ctx, c, path, p)
	}
	result := studioResult(data, err)
	// These legacy Studio APIs deliberately return a top-level object, not an envelope.
	if path == "/producer/connection" && err == nil {
		for k, v := range jsonObject(data) {
			result[k] = v
		}
	}
	return result
}
func studioPage(rows []any, p map[string]any) map[string]any {
	page := maxValue(1, intValue(p, 1, "page"))
	size := intValue(p, 20, "pageSize")
	if size < 1 || size > 1000 {
		size = 20
	}
	start := minValue(len(rows), (page-1)*size)
	end := minValue(len(rows), start+size)
	return map[string]any{"items": rows[start:end], "total": len(rows), "page": page, "size": size, "pageSize": size, "resultMayBeTruncated": false}
}
func studioRows(value any) []map[string]any {
	b, _ := json.Marshal(value)
	var rows []map[string]any
	_ = json.Unmarshal(b, &rows)
	return rows
}
func studioMatches(name string, p map[string]any) bool {
	if q := strings.ToLower(stringValue(p, "search", "keyword")); q != "" && !strings.Contains(strings.ToLower(name), q) {
		return false
	}
	if names := stringValue(p, "names"); names != "" {
		for _, n := range strings.Split(names, ",") {
			if n == name {
				return true
			}
		}
		return false
	}
	return true
}
func studioDate(ms int64) any {
	if ms <= 0 {
		return nil
	}
	return time.UnixMilli(ms).Format("2006-01-02 15:04:05")
}
