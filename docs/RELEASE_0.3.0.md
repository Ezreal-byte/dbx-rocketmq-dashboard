# dbx-rocketmq-dashboard 0.3.0

一个类似于 apache/rocketmq-dashboard 的 RocketMQ 驾驶舱。UI 和功能基于 [Apache RocketMQ Dashboard](https://github.com/apache/rocketmq-dashboard) 复刻，复用其原生 React 业务页面，以 Go 后端接入 DBX。

- 项目更名为 `dbx-rocketmq-dashboard`；插件 ID 保持不变，支持旧版身份识别。
- 插件图标和侧栏使用 RocketMQ Logo，保留新版原生 UI，移除 AI 首页及菜单。
- 新增 GitHub Actions：前端构建、许可证检查、Go 与传输测试、RocketMQ 4.9.7 / 5.3.3 集成测试、Windows x64 打包及完整性验证。
- Windows Sidecar 固定使用 Go 1.20.14，前端目标 WebView2 109。

提供的 `.dbxp` 是 Windows x64 未签名候选包，商店上架仍需 DBX Store 审核和签名。Windows 7 实机、原生文件保存及完整业务场景仍待验收，详细结果和限制见 [测试报告](https://github.com/Ezreal-byte/dbx-rocketmq-dashboard/blob/v0.3.0/docs/TEST_REPORT_0.3.0.md)。

本项目为独立社区插件，非 Apache Software Foundation 官方发行版。保留上游 Apache-2.0 LICENSE、NOTICE 及依赖许可证。
