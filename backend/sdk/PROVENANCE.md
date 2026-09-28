# Provenance

Vendored copy of DBX's Go plugin SDK (Protocol v1).

| Field | Value |
| --- | --- |
| Upstream path | `t8y2/dbx:plugins/sdk/go/dbx-plugin-sdk` |
| Upstream commit | `b288a6b5f423b6f96d2c9971678fe633560027bb` (2026-09-14) |
| Module path | `github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk` (unchanged) |
| License | Apache License 2.0 (DBX repository `LICENSE`, bundled verbatim) |

## Why this copy exists

Plugins must not depend on a network checkout of the host repository, and DBX's
Windows 7 compatible build uses `go1.20.14` while the upstream SDK declares
`go 1.22`. Go 1.20 refuses to build a module that requires a newer toolchain, so the
plugin carries its own copy with a lowered `go` directive.

## Local modifications

```diff
-module github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk
-
-go 1.22
+module github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk
+
+go 1.20
```

`go.mod` aside, `sdk.go`, `sdk_test.go` and `README.md` are byte-identical to the
upstream commit. Framed transport support is intentionally kept (the plugin declares
`stdio-jsonl` and never enables it) so that a future refresh stays a straight copy.

## Refresh procedure

1. Copy `sdk.go`, `sdk_test.go` and `README.md` from a newer DBX commit.
2. Re-apply the `go.mod` modification above and update the commit hash in this file.
3. Run `go test ./...` from this directory with `go1.20.14` before committing.
