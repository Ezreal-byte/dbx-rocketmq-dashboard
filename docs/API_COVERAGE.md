# 接口覆盖清单

已登记 54 个上游路径；所有业务请求经过 dashboard/request 的方法白名单和后端只读检查。来源：Dashboard 2.1.0 Controller/Service，固定提交见 PROVENANCE.md。

“通过”表示日志记录真实集群调用成功，不表示所有参数组合、错误分支及界面一致性均已穷尽验收。批量部分失败、权限、隔离、导出另见测试报告。ACL 2.0 在 4.9.x 是服务端不支持。

| 页面操作 | 上游方法 / 路径 | Go 实现 | 验收用例 | 4.9.7 | 5.3.3 |
|---|---|---|---|---|---|
| 集群 / Broker | GET `/cluster/list.query` | dashboard.go | TestDashboardIntegration | 通过 | 通过 |
| 集群 / Broker | GET `/cluster/brokerConfig.query` | dashboard.go | TestDashboardIntegration | 通过 | 通过 |
| Topic 管理与发送 | GET `/topic/list.query` | dashboard_topics.go / dashboard_messages.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| Topic 管理与发送 | GET `/topic/list.queryTopicType` | dashboard_topics.go / dashboard_messages.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| Topic 管理与发送 | GET `/topic/stats.query` | dashboard_topics.go / dashboard_messages.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| Topic 管理与发送 | GET `/topic/route.query` | dashboard_topics.go / dashboard_messages.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| Topic 管理与发送 | GET `/topic/queryConsumerByTopic.query` | dashboard_topics.go / dashboard_messages.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| Topic 管理与发送 | GET `/topic/queryTopicConsumerInfo.query` | dashboard_topics.go / dashboard_messages.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| Topic 管理与发送 | GET `/topic/examineTopicConfig.query` | dashboard_topics.go / dashboard_messages.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| Topic 管理与发送（写） | POST `/topic/createOrUpdate.do` | dashboard_topics.go / dashboard_messages.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| Topic 管理与发送（写） | POST `/topic/deleteTopic.do` | dashboard_topics.go / dashboard_messages.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| Topic 管理与发送（写） | POST `/topic/deleteTopicByBroker.do` | dashboard_topics.go / dashboard_messages.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| Topic 管理与发送（写） | POST `/topic/sendTopicMessage.do` | dashboard_topics.go / dashboard_messages.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| 消费组、位点、客户端、监控入口 | GET `/consumer/groupList.query` | dashboard_consumers.go / dashboard_protocol.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| 消费组、位点、客户端、监控入口 | GET `/consumer/group.refresh` | dashboard_consumers.go / dashboard_protocol.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| 消费组、位点、客户端、监控入口 | GET `/consumer/group.refresh.all` | dashboard_consumers.go / dashboard_protocol.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| 消费组、位点、客户端、监控入口 | GET `/consumer/group.query` | dashboard_consumers.go / dashboard_protocol.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| 消费组、位点、客户端、监控入口 | GET `/consumer/examineSubscriptionGroupConfig.query` | dashboard_consumers.go / dashboard_protocol.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| 消费组、位点、客户端、监控入口 | GET `/consumer/fetchBrokerNameList.query` | dashboard_consumers.go / dashboard_protocol.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| 消费组、位点、客户端、监控入口 | GET `/consumer/queryTopicByConsumer.query` | dashboard_consumers.go / dashboard_protocol.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| 消费组、位点、客户端、监控入口 | GET `/consumer/consumerConnection.query` | dashboard_consumers.go / dashboard_protocol.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| 消费组、位点、客户端、监控入口 | GET `/consumer/consumerRunningInfo.query` | dashboard_consumers.go / dashboard_protocol.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| 消费组、位点、客户端、监控入口（写） | POST `/consumer/createOrUpdate.do` | dashboard_consumers.go / dashboard_protocol.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| 消费组、位点、客户端、监控入口（写） | POST `/consumer/deleteSubGroup.do` | dashboard_consumers.go / dashboard_protocol.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| 消费组、位点、客户端、监控入口（写） | POST `/consumer/resetOffset.do` | dashboard_consumers.go / dashboard_protocol.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| 消费组、位点、客户端、监控入口（写） | POST `/consumer/skipAccumulate.do` | dashboard_consumers.go / dashboard_protocol.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| 生产者连接 | GET `/producer/producerConnection.query` | dashboard_protocol.go | TestDashboardLiveIntegration | 通过 | 通过 |
| 消息查询 / 详情 / 直接消费 | GET `/message/viewMessage.query` | dashboard_messages.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| 消息查询 / 详情 / 直接消费 | GET `/message/queryMessageByTopicAndKey.query` | dashboard_messages.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| 消息查询 / 详情 / 直接消费 | GET `/message/queryMessageByTopic.query` | dashboard_messages.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| 消息查询 / 详情 / 直接消费 | POST `/message/queryMessagePageByTopic.query` | dashboard_messages.go | TestDashboardIntegration / LiveIntegration | 通过（分页/正文断言） | 通过（分页/正文断言） |
| 消息查询 / 详情 / 直接消费（写） | POST `/message/consumeMessageDirectly.do` | dashboard_messages.go | TestDashboardIntegration / LiveIntegration | 通过 | 通过 |
| 消息轨迹 | GET `/messageTrace/viewMessage.query` | dashboard_messages.go / dashboard_trace.go | TestDashboardLiveIntegration | 通过 | 通过 |
| 消息轨迹 | GET `/messageTrace/viewMessageTraceGraph.query` | dashboard_messages.go / dashboard_trace.go | TestDashboardLiveIntegration | 通过 | 通过 |
| 死信查询 / 导出 / 重发 | POST `/dlqMessage/queryDlqMessageByConsumerGroup.query` | dashboard_messages.go | TestDashboardLiveIntegration | 通过 | 通过 |
| 死信查询 / 导出 / 重发 | GET `/dlqMessage/exportDlqMessage.do` | dashboard_messages.go | TestDashboardLiveIntegration | 通过 | 通过 |
| 死信查询 / 导出 / 重发（写） | POST `/dlqMessage/batchResendDlqMessage.do` | dashboard_messages.go | TestDashboardLiveIntegration | 通过 | 通过 |
| 死信查询 / 导出 / 重发 | POST `/dlqMessage/batchExportDlqMessage.do` | dashboard_messages.go | TestDashboardLiveIntegration | 通过 | 通过 |
| ACL 2.0 用户 / 策略 | GET `/acl/users.query` | dashboard_acl.go | TestDashboardLiveIntegration | 不支持 ACL 2.0（错误已验证） | 通过 |
| ACL 2.0 用户 / 策略 | GET `/acl/acls.query` | dashboard_acl.go | TestDashboardLiveIntegration | 不支持 ACL 2.0（错误已验证） | 通过 |
| ACL 2.0 用户 / 策略（写） | POST `/acl/createUser.do` | dashboard_acl.go | TestDashboardLiveIntegration | 不支持 ACL 2.0（错误已验证） | 通过 |
| ACL 2.0 用户 / 策略（写） | POST `/acl/updateUser.do` | dashboard_acl.go | TestDashboardLiveIntegration | 不支持 ACL 2.0（错误已验证） | 通过 |
| ACL 2.0 用户 / 策略（写） | DELETE `/acl/deleteUser.do` | dashboard_acl.go | TestDashboardLiveIntegration | 不支持 ACL 2.0（错误已验证） | 通过 |
| ACL 2.0 用户 / 策略（写） | POST `/acl/createAcl.do` | dashboard_acl.go | TestDashboardLiveIntegration | 不支持 ACL 2.0（错误已验证） | 通过 |
| ACL 2.0 用户 / 策略（写） | POST `/acl/updateAcl.do` | dashboard_acl.go | TestDashboardLiveIntegration | 不支持 ACL 2.0（错误已验证） | 通过 |
| ACL 2.0 用户 / 策略（写） | DELETE `/acl/deleteAcl.do` | dashboard_acl.go | TestDashboardLiveIntegration | 不支持 ACL 2.0（错误已验证） | 通过 |
| 消费监控配置 | GET `/monitor/consumerMonitorConfigByGroupName.query` | dashboard_metrics.go | TestDashboardIntegration | 通过 | 通过 |
| 消费监控配置 | GET `/monitor/consumerMonitorConfig.query` | dashboard_metrics.go | TestDashboardIntegration | 通过 | 通过 |
| 消费监控配置（写） | POST `/monitor/createOrUpdateConsumerMonitor.do` | dashboard_metrics.go | TestDashboardIntegration | 通过 | 通过 |
| 消费监控配置（写） | POST `/monitor/deleteConsumerMonitor.do` | dashboard_metrics.go | TestDashboardIntegration | 通过 | 通过 |
| 统计 / 历史图表 | GET `/dashboard/broker.query` | dashboard_metrics.go | TestDashboardIntegration | 通过 | 通过 |
| 统计 / 历史图表 | GET `/dashboard/topic.query` | dashboard_metrics.go | TestDashboardIntegration | 通过 | 通过 |
| 统计 / 历史图表 | GET `/dashboard/topicCurrent.query` | dashboard_metrics.go | TestDashboardIntegration | 通过 | 通过 |
| 消费者 Proxy 选择（DBX 连接设置） | GET `/proxy/homePage.query` | dashboard.go / dashboard_protocol.go | TestDashboardIntegration（配置读取），真实 Proxy 待验收 | 通过 | 通过 |

DBX 生命周期：connection/test、connection/connect、connection/disconnect。宿主导出：filesystem/download/stage、append、open、read、close；大文件阶段传输和跨连接拒绝由 TestDashboardStagedExport / TestDashboardDownloadIsolationAndClose 验证。

原始页面文件 9 个全部保留。移除的 Login、Ops、Proxy 页面是已约定的宿主适配：连接配置迁入 DBX，不以隐藏业务入口规避实现。监控配置不添加上游未启用的报警调度任务。
