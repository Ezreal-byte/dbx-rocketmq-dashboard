# Ported code

The RocketMQ protocol work in this directory is a snapshot of DBX's built-in
native agent, so the plugin reproduces the built-in console's behaviour instead
of re-deriving RocketMQ's remoting protocol.

| Field | Value |
| --- | --- |
| Source | `t8y2/dbx:agents/drivers/rocketmq` |
| Commit | `19b0c49db4f9fb11a56b706ea397d5d7638819dc` (2026-09-09) |
| Copied as | every `*.go` file of that directory, including its tests |

DBX keeps its own copy: the plugin is a separate deliverable, and the built-in
console stays untouched (see the project README). The two will drift; the table
below is the record of what already differs, and it must be extended whenever a
file is adapted further.

## Differences applied on import

| File | Change | Why |
| --- | --- | --- |
| `helpers.go` | Added generic `minValue` / `maxValue` helpers | The agent was written against Go 1.21+, where `min`/`max` are builtins. This plugin is built with `go1.20.14` for Windows 7 support. |
| `consumers.go`, `messages.go`, `topics.go` | Replaced the 16 `min(...)` / `max(...)` builtin calls with `minValue` / `maxValue` | Same reason. |

Nothing else was modified on import. The remaining adaptation (single-process,
single-connection agent to a plugin sidecar with one session per connection) is
described in the next section and lands with the plugin's own files.

## Structural changes still to come

The upstream agent is one process per connection (`rocketMQAgent` holds a single
`*admin.Client`). A DBX plugin is one persistent sidecar process for the whole
plugin, so a session per `connection.id` is required:

| Upstream | Plugin |
| --- | --- |
| `main.go` JSON-RPC loop with a `{"ready":true}` banner | `dbx-plugin-sdk` v1 server (`plugin/initialize` handshake, stderr-only logging) |
| `server.go` `handshake` / `connect` / `test_connection` / `disconnect` / `shutdown` | Fixed lifecycle methods `connection/test`, `connection/connect`, `connection/disconnect` |
| `server.go` `rocketMQAgent` singleton state | `session` per connection, keyed by `connection.id` |
| `parseConnection` `socks_proxy` (from DBX's Rust core) | Additionally accepts the host-provided `runtime.proxy` (SOCKS5 route for multi-endpoint connections) |
| `mq_*` methods | Unchanged names and payloads, so the ported UI keeps working |
