# 0.3.2 ACL 回归报告

日期：2026-10-26。针对 issue #2，使用隔离 Docker Broker；未连接报告者的远程服务器。

## 根因与修复

| 现象 / 同类问题 | 修复 | 验证 |
| --- | --- | --- |
| Broker 的 `Allow` 在页面显示为拒绝，`Pub/Sub` 未选中 | 大小写转换，保留管理类操作权限 | Go 实际返回断言；React 页面选中状态 |
| `Topic:*` / `Group:*` 读成空资源，更新变成 `**` | 显式 ANY 模式与规范化资源键 | 通配、前缀、全局资源创建、回显、更新 |
| Default/Custom 同资源的 ID 冲突、删除固定落在 Custom | ID 包含策略类型，删除携带 policyType；兼容旧两段 ID | 同主体五条规则逐一删除、逐次验证其他规则仍存在 |
| 规则写入固定清空 sourceIps | 原生表格和弹窗支持白名单，后端校验 IP/CIDR | 保存后重新读取 Broker；显式清空；缺字段/null/错误类型/非法 IP 拒绝写入 |
| 新建用户缺少密码，编辑重新启用已禁用用户 | 必填创建密钥；更新只提交明确修改的字段 | 创建、修改管理员、轮换密钥；检查密码、权限、禁用状态保留 |
| 凭据查看调用未注册接口 | 接入 GetUser，仅显式查看返回密钥，跨 Broker 不一致时报错 | 实际查看和轮换后读取；普通用户列表不泄漏密钥 |
| 操作失败只显示通用提示、本地元数据说明错误 | 保留 Broker 错误详情；纠正说明 | 页面失败提示与失败后弹窗保留 |

## 已执行

- `npm test`：12 项桥接、路由和打包 HTML 检查通过。
- ACL React 页面、API、service、诊断测试：4 个文件、44 项通过。jsdom 不实现伪元素的提示不影响断言。
- TypeScript、Vite 构建与许可证检查通过：62 个实际组件、69 个许可证文件。前端没有新增依赖。
- Go 1.20.14：5.5.0 环境下 `go test -count=1 -timeout 180s -v ./...` 通过；SDK 测试通过。需要独立认证/TLS 环境等条件的现有测试按其条件跳过。
- RocketMQ 4.9.7、5.3.3、5.5.0 的 `TestStudio*` 集成测试通过，包含之前的 Topic 可见性回归。4.9.7 跳过 ACL 2.0 专项，因为该版本不提供 ACL 2.0 协议。
- 5.3.3 与 5.5.0 ACL 回归均执行直连和 SOCKS5 两组真实写入/读回/删除，确认 NameServer 及发现的 Broker 经过代理。5.5.0 全量测试另覆盖 VIP 端口和连接隔离。
- 本地 DBX 开发宿主加载编译后的 Go Sidecar 与内嵌 UI，检查 ACL 表格、原生弹窗及 IP/CIDR 标签录入。没有使用模拟数据替代 Broker 回归。

## 复现方法

```powershell
docker compose -f tests/compose.yaml up -d namesrv55 broker55
# 等待日志出现 boot success
$env:ROCKETMQ_INTEGRATION='1'
$env:ROCKETMQ_NAMESRV_ADDR='127.0.0.1:49876'
$env:ROCKETMQ_VERSION='5.5.0'
Push-Location backend
go test -count=1 -timeout 180s -v -run TestStudioACL ./...
Pop-Location
npm --prefix web test -- src/pages/instance/__tests__/AclPage.test.tsx src/api/acl.test.ts src/services/aclService.test.ts src/utils/aclRiskDiagnostics.test.ts
```

测试创建独立命名的用户和规则并清理。5.x 夹具启用认证/授权元数据 Provider，但未开启请求鉴权强制执行；白名单验证是 Broker 持久化及协议回归，不代表对报告者网络环境下的鉴权放行/拒绝作实测。SOCKS5 测试使用本地真实转发代理，不是远程 SSH 服务器验收。

发布流水线固定 Go 1.20.14，执行 Windows x64 打包、PE 导入及包内容哈希检查；这不替代 Windows 7 + WebView2 109 实机验证。其余功能限制仍见 `TEST_REPORT_0.3.0.md`。
