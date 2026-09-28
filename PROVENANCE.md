# Source provenance

This is an independent DBX plugin, not an Apache Software Foundation release.

## 0.3.0 naming and artwork

The project is now named `dbx-rocketmq-dashboard`; the existing plugin ID is preserved for upgrades. Its UI and functional scope replicate Apache RocketMQ Dashboard, with the Go/DBX integration maintained independently. The RocketMQ SVG supplied by the project owner is copied unchanged to `assets/rocketmq.svg` and `web/src/assets/rocketmq.svg`. Apache RocketMQ names and artwork identify the upstream technology and do not imply ASF endorsement. Current verification and limitations are recorded in `docs/TEST_REPORT_0.3.0.md`.

## Active UI in 0.2.0

* RocketMQ Studio 3.0 `web/`, from Apache RocketMQ Dashboard master commit `0228dad5b9c9460f3e18c5c3e2b56c6525856198`.
* Exported from the official Git repository; original source headers, LICENSE, NOTICE and web dependency lock retained.
* React, Ant Design, Tailwind, original page layouts, tables, dialogs, filters, themes and language system are reused. Local changes add DBX invoke transport, a connection gate, memory routing, native downloads and sandbox-safe storage. Console login, mock mode, AI home/navigation and platform-only navigation are removed. Home uses the upstream monitoring overview.
* Windows archive extraction converted license files to CRLF. `web/licenses/` is normalized back to LF so the original upstream checksum gate passes without changing its expected hashes.
* The previous 2.1.0 `frontend/` remains as historical source, but is no longer built or used by the 0.2.0 package. The new package embeds only `ui/index.html`; license notices from the actual Vite bundle are under `licenses/studio-web/`.
* The new `studio/request` Go adapter is an integration candidate. See `docs/STUDIO_0.2.0_REVIEW.md` for verified operations and outstanding validation.

## Historical 0.1.0 source

* Upstream: https://github.com/apache/rocketmq-dashboard
* Tag: `rocketmq-dashboard-2.1.0`
* Commit: `a013c8fad163122713b0bd2db3d73b3bf0db373e`
* Original checkout: `E:\xynanan\rocketmq-dashboard`
* `frontend/` originates from that commit's `frontend-new/`. Apache LICENSE, NOTICE and source copyright headers are retained. The dependency lock is regenerated and committed as source, because the build needs the DBX browser target and local package name.
* Initial Go backend, SDK, vendored admin library and packaging tools were copied from `E:\xynanan\dbx-plugin-rocketmq`. That local directory has no Git repository; it is not represented as a clean upstream revision. `docs/legacy-source-hashes.json` records the current source baseline for comparison. The old project has not been modified by this task.
* Admin dependency: `github.com/amigoer/rocketmq-admin-go v1.1.1`, local replacement in `backend/third_party/rocketmq-admin-go`; its LICENSE is retained. The Remoting disconnect and NameServer failover fixes are local changes.
* Client dependency: `github.com/apache/rocketmq-client-go/v2 v2.1.2`. The SDK is locally replaced at `backend/sdk`; its license is retained.

Local frontend changes: DBX bridge and lifecycle gate; MemoryRouter for opaque `about:srcdoc` iframe; removal of Login/Ops/Proxy configuration pages and logout/session redirects; host download API; sandbox-safe theme preferences; connection-specific Proxy configuration; byte-exact exports; missing batch DLQ export API; ACL broker parameter and body encoding fixes; trace status/cluster role display fixes; real timestamp charts with missing-sample gaps. All nine business pages remain.

Go compatibility behavior follows the pinned Dashboard Java Controllers/Services. Trace decoding is based on `MsgTraceDecodeUtil` and the RocketMQ trace protocol. ACL wire headers and statistics fallback were checked against Apache RocketMQ 5.3.3 source:

* https://github.com/apache/rocketmq/blob/rocketmq-all-5.3.3/client/src/main/java/org/apache/rocketmq/client/impl/MQClientAPIImpl.java
* https://github.com/apache/rocketmq/blob/rocketmq-all-5.3.3/tools/src/main/java/org/apache/rocketmq/tools/command/stats/StatsAllSubCommand.java

Run `node scripts/source-audit.mjs` to regenerate `docs/upstream-2.1.0.patch` and the file inventories. Review that patch when updating upstream; do not rerun the original one-shot adaptation scripts over an edited frontend.
