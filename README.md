# dbx-rocketmq-dashboard

<img src="assets/rocketmq.svg" width="72" alt="RocketMQ logo" />

**一个类似于 [apache/rocketmq-dashboard](https://github.com/apache/rocketmq-dashboard) 一样的 RocketMQ 驾驶舱，在 DBX 中管理 RocketMQ。**

本项目的 **UI 和功能复刻自 Apache RocketMQ Dashboard**：直接复用其 RocketMQ Studio 原生 React 页面、组件、样式和业务交互，用 Go 重写后端接口，通过 DBX invoke 调用。运行插件不需要 Java，也不需要另外部署 Dashboard HTTP 服务。本项目是独立的社区 DBX 插件，并非 Apache Software Foundation 官方发行版。

当前版本：**0.3.0**，Windows x64。插件 ID 保持为 `io.dbx.rocketmq-dashboard-console`，便于从早期 `dbx-rocketmq-console` 升级。源代码采用 Apache-2.0 许可证，保留上游 LICENSE、NOTICE 和版权头。

## 界面和功能

前端来自官方仓库的 `web/`，固定提交 [`0228dad5b9c9460f3e18c5c3e2b56c6525856198`](https://github.com/apache/rocketmq-dashboard/tree/0228dad5b9c9460f3e18c5c3e2b56c6525856198/web)。复用原生侧栏、主题、国际化、表格、筛选、分页、弹窗和图表；首页使用监控总览，移除 AI 首页和菜单。连接与凭据统一由 DBX 管理，不再使用控制台登录页。

- 集群、Broker、NameServer 和客户端信息。
- Topic、消费组、订阅与消费进度、位点重置。
- 消息查询和发送、队列浏览、消息轨迹、死信处理。
- RocketMQ ACL 管理、连接期间的历史统计和文件导出。

这些是适配范围，不代表每项已经完成实机验收。当前通过本地测试的操作和已知限制见 [0.3.0 测试报告](docs/TEST_REPORT_0.3.0.md)。K8s、多云、告警通知平台和 AI 功能不在本插件范围内；云厂商专有管理 API 不在支持范围内。

## 安装与连接

从 [Releases](https://github.com/Ezreal-byte/dbx-rocketmq-dashboard/releases) 下载 Windows x64 `.dbxp`，在 DBX 中安装并创建 RocketMQ 连接。Release 提供未签名候选包；DBX Store 上架需要商店审核、签名后完成。

填写 NameServer 地址，多地址用分号分隔；按集群要求配置 AccessKey/SecretKey、TLS、VIP Channel。凭据由 DBX Secret Store 保存，仅后端接收。SSH/SOCKS5 使用 DBX 提供的代理路由，发现的 Broker 也走该路由。连接成功后直接进入工作台。

`proxy_addr` 指 RocketMQ **Remoting Proxy**，不是 gRPC 端口。只读连接的写操作由 Go 后端统一拒绝。4.9.x 不支持 ACL 2.0 管理协议，接口会返回实际能力错误；4.9.x 的 ACL 凭据认证仍可使用。

本地隔离测试连接（需先启动下文的 Docker 服务）：

| 版本 | NameServer | AccessKey / SecretKey |
| --- | --- | --- |
| 4.9.7 | `127.0.0.1:19876` | 留空 |
| 5.3.3 | `127.0.0.1:29876` | 留空 |

## 构建与测试

使用 Node.js **22.13+**、npm、固定 **Go 1.20.14**。Windows 构建启用 PE 导入检查，前端目标为 Chrome/WebView2 109；这不等同于 Windows 7 实机运行验证。

```powershell
npm ci --prefix web --ignore-scripts
npm run build
npm test
go -C backend test -count=1 -timeout 180s ./...
go -C backend/sdk test ./...
node scripts/third-party-licenses.mjs
go run scripts/package-cross.go -target windows-x64
./scripts/verify-package.ps1
node scripts/release-metadata.mjs
```

需要指定 Go 安装时，设置 `GO_BIN`，并把其 `bin` 目录加入 `PATH`。产物位于 `dist/`。GitHub Actions 在主分支和 PR 上运行构建与 4.9.7 / 5.3.3 集成测试；推送匹配 manifest 版本的 `v*` 标签后，全部检查通过才发布 Release。

```powershell
docker compose -f tests/compose.yaml up -d
# 等待 Broker 启动、注册后执行；测试会创建并清理独立的 Topic / Group。
$env:ROCKETMQ_INTEGRATION='1'
$env:ROCKETMQ_NAMESRV_ADDR='127.0.0.1:19876'
$env:ROCKETMQ_VERSION='4.9.7'
$env:ROCKETMQ_AUTH_NAMESRV='127.0.0.1:39876'
go -C backend test -count=1 -timeout 180s -v ./...
$env:ROCKETMQ_NAMESRV_ADDR='127.0.0.1:29876'
$env:ROCKETMQ_VERSION='5.3.3'
go -C backend test -count=1 -timeout 180s -v -run 'TestStudio|TestDashboard' ./...
```

`tests/plain_acl.yml` 中的账号仅为公开、隔离的测试夹具。Java 只用于测试 RocketMQ 服务端。开发预览使用 `node scripts/start-dev.mjs`，需相邻 DBX 项目的已构建开发宿主，或通过 `DBX_PLUGIN_DEV_RUNTIME` 指定它。

## 数据与权限

声明权限为 `host.workbench`。Sidecar 使用 stdio JSONL 与 DBX 通信，访问用户配置的 NameServer、Proxy 和发现的 Broker；管理客户端的转发桥会监听临时的本机回环 TCP 端口。运行时不启动 Java、外部命令或 HTTP 控制台。

统计和监控配置保存到 `DBX_PLUGIN_DATA_DIR/dashboard/<连接 ID 的 SHA-256>/`，未提供该路径时回退到用户缓存目录 `dbx/<插件 ID>/dashboard/`。连接期间每分钟采样，断连停止，历史缺口显示无数据；历史不自动删除。每连接最多 16 个消息查询任务，闲置一小时过期，断连清理。单任务最多 50,000 条消息，文件导出上限 8 MiB，请求总预算 55 秒。

来源与修改记录见 [PROVENANCE.md](PROVENANCE.md)。`frontend/` 和相关 2.1.0 文档、一次性迁移脚本作为历史参考保留，不参与当前 UI 构建；不要在已修改源码上重新执行迁移脚本。

问题反馈：[GitHub Issues](https://github.com/Ezreal-byte/dbx-rocketmq-dashboard/issues)。
