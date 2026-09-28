# Provenance

Vendored copy of `github.com/amigoer/rocketmq-admin-go`.

| Field | Value |
| --- | --- |
| Upstream module | `github.com/amigoer/rocketmq-admin-go` |
| Upstream version | `v1.1.1` |
| Upstream tag commit | see `go.sum` hash `h1:` recorded in `../../go.sum` |
| License | Apache License 2.0 (`LICENSE`, bundled verbatim) |
| Retrieved via | `go mod download github.com/amigoer/rocketmq-admin-go` (Go module cache) |

## Why this copy exists

The DBX RocketMQ native agent upstream (`dbx/agents/drivers/rocketmq`) consumes the
published module directly. That module declares `go 1.25.0`, and every published
version is the same or newer (`v1.0.0` declares `go 1.24.3` and even renames the
module path to `github.com/codermast/rocketmq-admin-go`). Go 1.21 dropped Windows 7
support, and this plugin must ship a Windows 7 compatible sidecar, so the library
cannot be consumed as a published module when the build toolchain is pinned to
`go1.20.14`.

This copy therefore exists only to lower the `go` directive and pin transitive
dependencies to releases that still build with Go 1.20. No library source file was
modified — see `PATCHES.md` for the exact diff.

## Refresh procedure

1. Download the new upstream version: `go mod download github.com/amigoer/rocketmq-admin-go@<version>`.
2. Re-copy the source into this directory (module cache files are read-only).
3. Re-apply the modifications listed in `PATCHES.md`.
4. Re-run `go build ./...`, `go vet .`, `go test .` and the Windows 7 PE audit with
   the `go1.20.14` toolchain before committing.
