# AAS v1.2 / PATCH-01 测试与验收矩阵

## 已执行的自动化测试

在本次 Linux + Python 3.13 环境下，使用 `PYTHONPATH=src pytest -q -ra`。核心测试基于真实 A/B 服务、临时磁盘目录与 SQLite，但 `Transport` 为进程内受控替身（未启动真实 SSH daemon）。正常画像由 Pillow 生成有效 PNG；故障测试按确定性注入 `KeyboardInterrupt` 模拟不可恢复的进程退出位置。

| PATCH 测试 | 覆盖证据 |
|---|---|
| T-P01 | 一次发现登记多个 READY；首次扫描无需等待周期 |
| T-P02 | `fixed_interval` 固定秒数、研究策略拒绝、成功空轮询单独计入 observations |
| T-P03 | 下载 worker 被阻塞时，发现线程继续扫描并登记第二个 READY |
| T-P04 | 页入库前崩溃，游标不前进，重启重取 |
| T-P05 | 页队列+游标提交后重启，仍按原队列完成正常交付 |
| T-P06 | SOURCE_CLEANING 删 1/N 源图后异常中止，A 审核密钥已销毁，恢复仅凭证书+密文 SHA 完成 READY |
| T-P07 | 改写已发布 bundle 一字节，停源删除并进入 `VERIFY_RECOVERY_BLOCKED` |
| T-P08 | 加密插件输出假成功但 payload 错误，VerifyGate 拒绝发布且 0 源图删除 |
| T-P09 | 假 ACK ID、hash、consumer、统计不符都拒绝，归档仍在 READY |
| T-P10 | DELIVERED 后 B 用户删除图片、ACK 先失败再成功，历史收据有效，不重新管理用户目录 |
| T-P11 | output rename 后/DELIVERED 前中断且用户移动目录，进入 `HANDOFF_UNCERTAIN`，无自动 ACK |
| T-P12 | 相同旧 seq 新 READY event id 被发现；full reconcile 仍可见 |
| T-P13 | SSH 超时不被统计成成功 0 批观测 |
| T-P14 | 积压跨页 106 个 READY，按分页事务全部登记，不超前游标 |
| T-P15 | ACK 后删除 bundle、中断在 manifest 删除前，再恢复完成 DELETED |

其他附加覆盖：端到端图片恢复、用户输出被删后 B SQLite 丢失/收据重建、相同 ID 内容漂移事务回滚、手动确认带审核记录、禁用旧 adaptive 配置、不可删未 ACK 归档、发布证书先于 rename 的中断、删完源目录但 DB 未 READY、中断在删除 ready 目录但 DB 未 DELETED、人工证书修复、SSH forced-command 白名单/身份隔离、应急长度帧与单图 SHA/ACK/原图 unlink、应急与 normal 领取排他、应急失败文件安全恢复、独立 ledger 对账。

## 真正部署后仍必须做的 P0/P1 环境验收

1. **真实 age 测试**：B 主/备 key 各解开 A 已发布密文；随机篡改 cipher 一字节必须在 A 或 B 的 VerifyGate/hash 失败；A 审核私钥销毁后重启 SOURCE_CLEANING 可以续删。
2. **真实受限 SSH 测试**：确保没有可执行 shell 或任意路径读取；正常密钥不能 emergency；应急密钥不能 ACK；`StrictHostKeyChecking=yes` 预期 host key 改变必须阻断。
3. **Windows B**：真实 NTFS same-volume rename、文件名保留字、目录占用、跨卷拒绝、进程突然结束后 PREPARED 恢复。
4. **性能/负载**：4GiB 包、10K PNG 多批积压、千页增量查询与无包周期、实际 SSH 建连成本、吞吐、`max_parallel_downloads=2` 与容量预留。
5. **断网及断电**：在 A final bundle 完整回读、证书落盘、ready rename、source unlink、ACK tombstone、bundle unlink、B rename/DELIVERED 的不同位置 kill -9；真正电源故障/盘坏是边界外，但真实文件系统需评价 fsync 行为。
6. **72h soak**：在持久 A 和 Windows/Linux B 上持续生成/轮询/断点续传，记录准备好的指标并人工审查状态/数据完整性。
7. **可观测性扩展项**：`poll_success_count`、`poll_empty_count`、网络失败数、ready 发现时间分位数、待办和吞吐、ACK_PENDING age、容量预计填满时间；当前 SQLite observations/audit_log 和 status 只提供其中一部分基础原始数据。

## 故障边界说明

本测试不证明 A 整卷毁损后可恢复，也不证明 B 用户交付目录在任何时刻都存在、不证明应急严格 exactly-once、不证明 Windows 目录元数据 fsync 与 Linux 相同。对“用户在 rename→DELIVERED 的极短窗口移动内容”的正确行为是判定 `HANDOFF_UNCERTAIN`，而不是假称自动修复。
