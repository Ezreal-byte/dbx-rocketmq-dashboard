# 验收报告：0.1.0 候选版

测试时间：2026-09-28 至 2026-09-29，Windows 开发机，Go 1.20.14。**代码和未签名安装包已交付，但尚未满足计划中的全部最终验收条件，不能标为完整生产验收通过。**

## 已执行

| 范围 | 实测结果 | 证据 |
|---|---|---|
| 原后端测试基线及新增测试 | 4.9.7 环境完整 `go test ./...`，52 个顶层用例通过、0 跳过；修复 SDK require、测试参数 JSON 化 | integration-4.log |
| Dashboard 5.3.3 | Dashboard 测试集通过；54 个登记路径的执行记录见接口清单 | integration-5.log、API_COVERAGE.md |
| 在线消费与死信 | 真实 Go push consumer 收到消息；运行信息/线程栈、直接消费、死信生成/查询/单条与批量导出/批量重发、在线重置通过 | live-4.9.7.log、live-5.3.3.log |
| 消息查询契约 | 消息 key/ID/时间范围、请求 1-based 与响应 0-based 分页、UTF-8 正文及首尾空白、真实存储大小/CRC 等元数据断言通过 | dashboard_integration_test.go、integration-4.log、integration-5.log |
| 轨迹 | 实际 SDK Pub 轨迹和插件发送的 Pub 轨迹在 Broker 中写入并查询成图；SubBefore/SubAfter/EndTransaction 解析有协议样本测试 | live-*.log、dashboard_contract_test.go |
| ACL 2.0 管理 | 5.3.3 开启本地 metadata provider 后，用户/策略增改查删通过；4.9.x 的服务端不支持错误已验证 | integration-5.log、live-*.log |
| ACL 认证 / 权限 | 独立 4.9.7 ACL Broker：管理员查询/创建/发送成功，错误或缺失凭据连接失败，非管理员写入被 Broker 拒绝 | TestDashboardAuthenticationIntegration |
| 部分失败 | 对两个真实测试 Broker 执行同一变更：正常 Broker 成功、ACL Broker 拒绝；返回包含成功及失败目标，成功端的配置实查存在 | TestDashboardRealPartialFailure（直接验证多 Broker 执行器，非同一集群发现测试） |
| 只读 / 隔离 / 断连 | 全部登记写接口拒绝只读连接；查询任务和导出跨连接拒绝；断连取消任务、清理缓存、停止采集且不影响另一连接 | dashboard_contract_test.go、dashboard_network_test.go |
| 路由 / 故障 | 真实 SOCKS5 转发 NameServer、发现的 Broker、VIP 端口；坏 NameServer 后切换正常节点；断开底层连接及时唤醒请求 | TestDashboardRoutingIntegration、TestRemotingDisconnectWakesPendingRequests |
| TLS | 实际 4.9.7/5.3.3 permissive TLS 服务器连通并查询集群；测试证书使用 skip verify | tls.log、integration-5.log |
| 历史 / 监控 | 实际采集与历史接口、监控配置持久化/连接隔离/删除、缺失日期不伪造数据；曲线按实际时间显示空白缺口 | integration-*.log、dashboard_contract_test.go、frontend-tests.log |
| 前端构建与桥接 | CRA 生产构建通过（保留上游 lint 警告）；8 个 Node 用例通过，包含全部业务 remoteApi 函数/路由登记、导出分块、HTML 脚本解析 | frontend-build.log、frontend-tests.log |
| DBX 开发宿主 UI | 原始九个业务入口保留，实际连接打开；Dashboard/Cluster/Consumer 真实查询；Topic 创建、中文消息发送、按消息 ID 弹窗查询通过；其他页面表单打开检查 | screenshots/、下文说明 |
| Windows 包 | 使用 go1.20.14 构建 Windows x64；PE 审查通过，仅静态导入 1 个模块；包文件 SHA256、产物摘要和入口校验 | package.log、package-verification.log |

54 个路径的调用成功覆盖不代表全部参数组合或全部错误分支已经穷尽。单元测试中的协议样本只用于契约验证；业务集成验收使用上述实际 Broker，无模拟成功或虚构业务数据。

## 尚未完成的最终验收

1. **Windows 7 SP1 + WebView2 109 实机运行**：当前没有该测试环境。设置编译目标和 PE 审查不能替代实机验收。
2. **原生 DBX 安装/卸载、关闭重开及 Secret Store/下载文件的端到端验收**：已在 DBX 开发宿主验证 stdio sidecar 和 sandbox iframe；原生桌面文件保存能力不可由开发宿主模拟认定通过。
3. **官方 Dashboard 2.1.0 逐页逐弹窗视觉对照**：已检查源代码保留和多页运行，并实际操作创建/发送/查询；未运行一套官方 Java Dashboard 完成全部截图差异比较。部分上游英文标签/空标签仍按原代码保留。
4. **真实 SSH 隧道、RocketMQ Remoting Proxy、证书校验 / 双向 TLS**：SOCKS5/VIP 和 TLS skip-verify 通路已实测；上述完整部署场景未验收。插件未增加客户端证书配置。
5. **大规模、长时间、多 Broker 拓扑故障测试**：已测两真实 Broker 部分失败执行器；同一集群多 Broker 发现、混合版本、长时间并发切换及满负载内存/超时仍需验证。
6. **5.x 强制认证部署、所有消息类型及客户端组合**：ACL 2.0 CRUD 在基础测试集群完成，强制认证使用 4.9 ACL 测试。事务提交/回查、FIFO/延时消息、消费侧真实轨迹等组合仍需专项验收。

## 实现边界与复现说明

* 测试 Broker 是 apache/rocketmq:4.9.7 和 :5.3.3 的本机隔离容器。5.3.3 显式启用本地 ACL metadata provider，基础集群不启用强制认证。ACL 凭据认证单独在 `broker-auth` 验证。
* Go SDK 2.1.2 的直接消费回调不自动将 DLQ 映射到原 Topic；测试客户端显式订阅 DLQ 以验证成功分发。不支持的客户端回调返回明确失败，插件不会把非成功 consumeResult 包装为成功。
* 上游消费者监控调度未启用；本次保留监控配置的实际读写，不声称增加了后台通知服务。
* 消息查询单任务最多 50,000 条、每连接最多 16 个任务，55 秒请求预算；导出最多 8 MiB，按块传输。大量大消息的内存压力未验收。历史磁盘文件目前无自动保留期清理。
* 当前历史曲线修复了上游分类轴按数组下标错位的问题，保留图表交互但按真实时间且不跨缺口连线。主题偏好仅保存在工作台内存中。
* screenshots/ui-send-success.png 是通过原始 Topic 页面发送真实中文消息的结果；ui-message-detail.png 使用从最终 `.dbxp` 提取的二进制复验，实际显示存储大小 284 bytes、CRC 1143843924、重试次数 0 及中文正文，浏览器未记录运行错误。开发宿主在 CRA 重建目录时出现的 watcher 提示属于开发服务，不是发行包的运行依赖；重启开发服务后消失。
* 不应把本报告中的“通过”外推为上述尚未执行项目通过。下一阶段应先安排 Win7/WebView2 109 与原生 DBX 的实际验收，再决定是否发布正式版本。
