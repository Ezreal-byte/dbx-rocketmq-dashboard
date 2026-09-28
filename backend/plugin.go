package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	sdk "github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk"
)

// plugin owns the session table of the sidecar. DBX keeps one sidecar process
// per plugin and reuses it for every connection, so sessions are keyed by
// `connection.id` and lifecycle transitions are serialized to keep a reconnect
// from replacing a session that is still being built.
type plugin struct {
	lifecycle sync.Mutex
	mu        sync.RWMutex
	sessions  map[string]*session
}

func newPlugin() *plugin { return &plugin{sessions: make(map[string]*session)} }

func (p *plugin) Handle(_ sdk.RequestContext, method string, raw json.RawMessage, _ *sdk.Emitter) (interface{}, *sdk.PluginError) {
	switch method {
	case "connection/test", "connection/connect", "connection/disconnect":
		var value lifecycleParams
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, sdk.NewError(-32602, "invalid connection parameters")
		}
		result, err := p.handleLifecycle(method, value)
		if err != nil {
			return nil, sdk.NewError(-32000, err.Error())
		}
		return result, nil
	case "plugin/initialize":
		// Handled by the SDK server before requests reach a handler.
		return nil, sdk.MethodNotFound(method)
	}

	params, err := decodeParams(raw)
	if err != nil {
		return nil, sdk.NewError(-32602, "invalid request parameters")
	}
	target := p.session(stringValue(params, "connectionId"))
	if target == nil {
		return nil, sdk.NewError(-32000, "connection is not open; reconnect from DBX")
	}
	if method == "rocketmq/info" {
		return target.info(), nil
	}
	if method == "dashboard/request" {
		return target.dashboardRequest(params), nil
	}
	if method == "studio/request" {
		return target.studioRequest(params), nil
	}
	if strings.HasPrefix(method, "filesystem/download/") {
		value, err := target.dashboardDownload(method, params)
		if err != nil {
			return nil, sdk.NewError(-32000, err.Error())
		}
		return value, nil
	}
	if !isReadMethod(method) && !isWriteMethod(method) {
		return nil, sdk.MethodNotFound(method)
	}
	if isWriteMethod(method) {
		if err := target.writeAllowed(); err != nil {
			return nil, sdk.NewError(-32000, err.Error())
		}
	}
	target.operations.RLock()
	defer target.operations.RUnlock()
	result, err := target.dispatch(method, params)
	if err != nil {
		return nil, sdk.NewError(-32000, err.Error())
	}
	return result, nil
}

func (p *plugin) handleLifecycle(method string, value lifecycleParams) (interface{}, error) {
	if method == "connection/disconnect" {
		// Idempotent: disconnecting an unknown or already closed connection is
		// not an error, it is the desired end state.
		p.lifecycle.Lock()
		defer p.lifecycle.Unlock()
		p.drop(value.Connection.ID)
		return map[string]interface{}{"success": true}, nil
	}

	if value.Connection.ID == "" {
		return nil, errors.New("connection id is required")
	}

	p.lifecycle.Lock()
	defer p.lifecycle.Unlock()
	candidate, err := newSession(value)
	if err != nil {
		return nil, err
	}
	if err := candidate.probe(); err != nil {
		candidate.close()
		return nil, err
	}
	if method == "connection/test" {
		info := candidate.info()
		candidate.close()
		return map[string]interface{}{
			"success": true,
			"message": fmt.Sprintf("Connected to RocketMQ cluster %s", info.ClusterName),
			"info":    info,
		}, nil
	}

	info := candidate.info()
	p.mu.Lock()
	previous := p.sessions[value.Connection.ID]
	p.sessions[value.Connection.ID] = candidate
	p.mu.Unlock()
	candidate.startDashboardCollector()
	if previous != nil {
		previous.close()
	}
	return map[string]interface{}{"success": true, "info": info}, nil
}

func (p *plugin) session(connectionID string) *session {
	if connectionID == "" {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.sessions[connectionID]
}

func (p *plugin) drop(connectionID string) {
	if connectionID == "" {
		return
	}
	p.mu.Lock()
	target := p.sessions[connectionID]
	delete(p.sessions, connectionID)
	p.mu.Unlock()
	if target != nil {
		target.close()
	}
}

func decodeParams(raw json.RawMessage) (map[string]any, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return map[string]any{}, nil
	}
	var params map[string]any
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	return params, nil
}
