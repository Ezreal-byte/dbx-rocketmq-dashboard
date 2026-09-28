# Local modifications

Applied on top of upstream `github.com/amigoer/rocketmq-admin-go` `v1.1.1`.
Keep this list in sync with the copy; re-apply it after every upstream refresh.

## 1. `go.mod`: toolchain downgrade (required for Windows 7)

```diff
-module github.com/amigoer/rocketmq-admin-go
-
-go 1.25.0
+module github.com/amigoer/rocketmq-admin-go
+
+go 1.20
```

Rationale: the plugin sidecar is built with `go1.20.14`, the last Go release that
supports Windows 7. `go 1.25.0` makes Go 1.20 refuse to build the module.

## 2. `go.mod`: transitive dependency pin

```diff
-	golang.org/x/sys v0.42.0 // indirect
+	golang.org/x/sys v0.0.0-20220722155257-8c9f86f7a55f // indirect
```

Rationale: `golang.org/x/sys` `v0.42.0` requires a toolchain newer than Go 1.20.
The pinned pseudo-version is the one upstream `rocketmq-admin-go` `v1.0.0` used and
it builds cleanly with `go1.20.14`. The root module also pins `x/sys` because a
higher version can still be selected transitively by `sirupsen/logrus` and
`go.uber.org/atomic`; verify with `go list -m golang.org/x/sys` if the build graph
changes.

## 3. Removed files (no source change)

| Removed | Reason |
| --- | --- |
| `*_test.go` (13 files, package `admin`) | Require a live RocketMQ cluster (`localhost:9876`) and would make `go test ./...` fail for the plugin module. Behaviour is covered by the plugin's own tests and integration runs against a real cluster. `protocol/remoting/remoting_test.go` is self-contained and is kept. |
| `test_helper.go` | Only backs the upstream tests; imports `testing` in a non-test file. |
| `examples/` | Upstream usage samples, not needed in the plugin and never packaged. |
| `docs/logo.png` | 10 KB binary asset; `README.md` still references it, so expect a broken image link when reading the vendored README. |

## 4. Source files

No `.go` file inside this directory was modified. All Go 1.21+ language level
fallout was on the plugin's own side (`backend/helpers.go` gained
`minValue`/`maxValue`), not here.
