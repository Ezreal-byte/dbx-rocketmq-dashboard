# dbx-rocketmq-dashboard 0.3.2

修复 [#2：ACL 管理修改、删除、编辑异常](https://github.com/Ezreal-byte/dbx-rocketmq-dashboard/issues/2)。

- 正确转换 Broker 的 `Allow/Deny`、`Pub/Sub/All` 等枚举，避免允许规则误显示为拒绝、编辑时权限未选中。
- 保留 `Topic:*`、`Group:*`、全局 `*` 和前缀匹配的真实含义；不再读成空资源或写入双星号。规则 ID 包含策略类型，编辑和删除 Default/Custom 规则不会混淆；删除一条规则保留其他规则。
- 在原生 ACL 表格和弹窗加入来源 IP 白名单，支持 IP/CIDR、回显、保存和显式清空。编辑请求缺少白名单时拒绝写入，避免意外放开限制。SSH 场景以 Broker 实际看到的来源 IP 为准。
- 用户创建增加必填 Secret Key，编辑留空保留旧密钥，支持显式轮换和凭据查看。编辑不会重新启用已禁用用户；普通列表不返回密钥。
- 显示具体 Broker 错误，纠正“仅保存在 Studio 本地”的过时说明。主体和资源为规则身份，编辑时固定；更换身份需新建规则后删除旧规则。
- 增加 RocketMQ 5.5.0 发布测试矩阵和 ACL 页面回归，覆盖直连及 SOCKS5 下的真实读写。

Windows x64，Go 1.20.14，未签名 `.dbxp`；商店分发需审核签名。测试范围见 [0.3.2 回归报告](TEST_REPORT_0.3.2.md)。没有对报告者的远程 SSH 集群或 Windows 7 实机作验收。
