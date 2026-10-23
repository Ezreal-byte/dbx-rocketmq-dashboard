# dbx-rocketmq-dashboard 0.3.1

修复 [#1：主题存在插件没有显示](https://github.com/Ezreal-byte/dbx-rocketmq-dashboard/issues/1)。

官方 Studio 页面按实例名称选择连接，而旧 Go 接口把 Topic 的 `instanceId` 固定返回为 `"1"`，导致页面显示 Topic 总数却过滤掉所有表格行。现在 Topic、Group、位点预览和能力接口都使用与实例列表一致的标识；未填写连接名称时使用集群名称。保留旧数字 ID 请求兼容及按 DBX 连接隔离，不更改 SSH/SOCKS5 路由和只读保护。

- 新增真实 Broker 回归：创建五个 Topic，验证分页总数与可见行一致，涵盖普通连接、SOCKS5 代理及未命名连接；确认 NameServer 和发现的 Broker 均经过代理。
- RocketMQ 4.9.7 / 5.3.3 集成测试由 Actions 在发布前执行。
- Windows x64，固定 Go 1.20.14；未签名 `.dbxp` 供安装验证，商店更新需维护者审核签名。

本次是显示问题修复，其他已知限制仍见 [0.3.0 测试报告](TEST_REPORT_0.3.0.md)。未对报告者的远程服务器或 Windows 7 实机作验收。
