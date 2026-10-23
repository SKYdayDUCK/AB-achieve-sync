# AAS v1.2 + PATCH-01 — Code Review / 实现审查报告

**审查对象**：本压缩包 `src/aas`、`tests/`、部署配置与文档。  
**基准文档**：`docs/reference/Adaptive_Archive_Sync_v1.2.md` + `docs/reference/AAS_PATCH-01.md`（补丁优先）。  
**审查范围**：业务状态机、持久化原子性、安全删除门槛、密码学职责边界、路径输入校验、SSH 授权、文件交付/ACK、故障恢复、调度并发及可运维性。  
**审查结论**：**本机主链路与 33 个自动化测试通过；当前尚不能宣称具备真实生产环境的完整 M6 验收**。真实 age、受限 SSH 线上行为、Windows NTFS、72h soak 和大文件性能必须在目标机器验证。

## 一、第一轮审查发现与已落实修复

| 编号 | 严重度 | 审查发现/风险 | 已实现修复与位置 | 结果 |
|---|---|---|---|---|
| CR-01 | P0 | 若删除源图后销毁临时 age 审核 key，进程重启不能再解密整包审查，可能卡死 SOURCE_CLEANING | `AService.package()` + `VerifyGate` 将完整验包 PASS 证书与 DB 哈希先持久化；`clean_sources()` 重启只校验最终密文/manifest/证书，拒绝不匹配 | 已修复、故障注入覆盖 |
| CR-02 | P0 | 插件可自行返回“验包成功”，导致源图被早删 | `VerifyGate.verify_final_bundle()` 自行对最终 bundle 解密、tar 成员及 SHA 全量校验；策略不拥有删除 API | 已修复、假成功测试通过 |
| CR-03 | P0 | READY 分页进度先于持久待办，进程异常永久跳过 batch | `BStore.apply_page()` 单个 `BEGIN IMMEDIATE` 同事务 UPSERT + cursor 更新；固定 epoch 与独立 event id | 已修复、断点前后/跨页/内容漂移测试通过 |
| CR-04 | P0 | A 源文件删一部分后崩溃，恢复依据不足导致乱删/跳过验包 | `clean_sources()` 限制 stage ID + inode/dev/size/mtime、重复校验证书、逐文件目录 fsync、删除审计；异常转 VERIFY_RECOVERY_BLOCKED | 已修复、部分删除测试通过 |
| CR-05 | P0 | 在最后源文件/归档目录删除后、状态提交前崩溃，正常恢复可能永远卡在中间态 | 缺失 staging 时必须有完整删除审计；DELETING 但 ready 批次目录已不存在时凭持久 ACK 墓碑补写 DELETED | 已修复、两种终点故障注入通过 |
| CR-06 | P0 | B 输出可修改，恢复过程中错误地认为文件缺失即未交付，可能重复交付或错误 ACK | PREPARED/DELIVERED 收据独立保存；PREPARED+目标缺失且 private 也缺失时 HANDOFF_UNCERTAIN；DELIVERED 后不扫描用户输出 | 已修复、用户移动/删除故障测试通过 |
| CR-07 | P0 | ACK 或归档删除可能被错误 batch/consumer/hash 或传输完成状态触发 | A 对 `producer, consumer, batch, seq, payload SHA, count, bytes` 强校验、持久 ACK CAS、删除白名单和墓碑 | 已修复、无有效 ACK 禁止删除测试通过 |
| CR-08 | P1 | 正常密钥也能发起手动 emergency，会扩大普通客户端能力 | SSH 强制命令增加 `consumer/emergency` 两个角色，B 使用单独 emergency SSH identity，角色间互拒 | 已修复、角色拒绝测试通过 |
| CR-09 | P1 | 应急领取与普通打包并发、失败残留恢复可能覆盖新图 | 两端共享 `input_claim.lock`；应急迁入 `emergency_claim`，按 file token 单独 ACK；恢复使用 no-clobber rename | 已修复、应急排他/重复风险测试通过 |
| CR-10 | P1 | A 包等待策略被简化为每 15s 必打一个包，会偏离 100 张/1 GiB/最长 600 秒设计 | `AConfig.max_wait_seconds` + `AService.claim()` 阈值判断；独立 `max_batch_bytes` 限制 | 已修复 |
| CR-11 | P1 | B 的解密临时 tar 若与下载盘不同，预检磁盘空间可能不足但仍开始操作 | `_restore()` 分别检查 incoming 明文临时容量与 restoring 图片容量，不影响 ACK 安全门槛 | 已修复 |
| CR-12 | P1 | `age` 操作对 4 GiB 包统一 300 秒超时，慢盘会触发假故障 | age 大文件加解密不硬编码 300s；keygen 仍限时，失败保留归档/源图 | 已修复，实际 age 二进制集成待测 |
| CR-13 | P1 | 再次公布旧 batch 时不应重置 seq 或覆盖已交付记录 | `reannounce-ready` 单独追加 READY event id；B 校验不可变内容，已有交付状态不回退 | 已修复、测试通过 |
| CR-14 | P1 | 数据库已丢但外部收据尚在，历史 DELIVERED 会被误判为未交付 | `BService.recover()` 从 DELIVERED JSON 补录 SQLite，按 ACK_PENDING 自主补发 ACK | 已修复、测试通过 |
| CR-15 | P2 | 旧 adaptive 参数可能被静默解释成 fixed，让用户误认为在线调度在运行 | B 配置拒绝 `scheduler.mode/min_poll/max_poll/poll_factor`；注册表只有 fixed_interval | 已修复、配置拒绝测试通过 |

## 二、按 P0 不变量进行代码级复审

| 设计不变量 | 实现与判定 |
|---|---|
| N1 源图删前完成真验包与持久发布 | `VerifyGate` + 证书 + `PUBLISHING→SOURCE_CLEANING`；源 unlink 只在 SOURCE_CLEANING；通过 |
| N2 READY 意味着 staging 对应源图已清理 | A `clean_sources()` 完成所有记录、删除目录后才 `mark_ready()` 分配事件 ID；通过 |
| N3 归档删除需要有效授权 ACK | `AStore.accept_ack()` 验身份并写 durable tombstone，`clean_bundle()` 检查持久 ACK；通过 |
| N4 B ACK 需要成功交付 + durable receipt | `BService._complete_delivered()` 先 fsync receipt、`_ack()` 再发 SSH ACK；通过 |
| N5 DELIVERED 不因用户更改撤回 | `BService.recover()` 和 `_ack()` 不检查用户当前 output；通过 |
| N6 应急与普通领取互斥 | 正常 claim 与 emergency session 都使用 A 的同一文件锁；通过 |
| N7 seq/event ID 分离；分页和对账无漏批 | SQLite `ready_events AUTOINCREMENT`、不可变批次校验、B 页队列/cursor 同事务、定期全量与 ledger；通过 |
| N8 不从网络路径推导任意删除 | `validate_id`、受限 SSH 命令、预定义 `ready/<id>` 与 `staging/<id>`，文件白名单 unlink；通过 |
| N9 崩溃续清理、ACK 幂等 | 多位置 fault injection、`already_acked/already_deleted`、独立收据索引恢复；**Linux 本机范围通过** |
| N10 非保证情形 | README/SECURITY/OPERATIONS 已披露；未虚称提供跨盘或用户可写目录严格 exactly-once |

## 三、自动化验证结果（本次实际执行）

- 命令：`PYTHONPATH=src python -m pytest -q -ra`。
- 结果：**33 passed, 1 skipped**；`skipped` 为真实 `age/age-keygen` 在本次容器环境不存在，不能用模拟实现替代密码学验证。
- 补充命令：`PYTHONPATH=src python scripts/local_smoke.py`：3 张 PNG 经 `A: READY → B: ACK_CONFIRMED → A: DELETED` 验证通过；源目录只读校验、B 输出三张图可见。
- `python -m compileall -q src tests scripts`、Python AST 语法检查通过；本机 `pip install --no-index --no-build-isolation -e .` 安装并能运行 `aas-a --help` 与 `aas-b --help`。
- **并非实测**：真正网络 SSH 端到端、age 实际 X25519 密文与备份密钥、Windows 平台 NTFS、真实 SSD 电源中断、4 GiB 性能、72h soak。`tests/` 使用受控 Transport 替身来证明业务门槛，不能等价替代上述现场验收。

## 四、仍未关闭的残余问题及优先级

| 编号 | 等级 | 尚需执行或改善 | 原因 / 影响 |
|---|---|---|---|
| OPEN-01 | P0 上线门槛 | 在安装 `age/age-keygen` 的两端进行真正的加密、临时审核身份销毁、恢复密钥、B 解密错密钥与私钥轮换测试 | 此环境没有 age 可执行程序，不能凭源码审查担保真实密码学工具链 |
| OPEN-02 | P0 上线门槛 | 使用两套受限 SSH key 在真实 `sshd` 下测试认证、错误 flag、二进制 get、host key 变更、应急权限 | 目前只有强制命令调度函数测试；真实网络部署配置需实证 |
| OPEN-03 | P0 上线门槛 | Windows B 的 NTFS rename/目录占用/崩溃恢复验证；72h 连续运行和磁盘不足演练 | Python 环境为 Linux，不能断言 Windows 行为已证实 |
| OPEN-04 | P1 性能 | 4GiB bundle 断点下载在每个 8MiB range 单独启动 SSH 的吞吐和 CPU 成本 | 可用但不是最高效；可后续加入受控 SSH 多路复用，不改可靠性状态机 |
| OPEN-05 | P1 可观测性 | 更完整的延迟分位数、吞吐分位数、时钟校正、容量预计填满时间和告警投递 | 目前 SQLite observations/audit_log 和 status 保存基础数据，暂未实现专门报表/告警集成 |
| OPEN-06 | P1 文件完成语义 | 生产端强制使用 `.tmp → fsync → rename`，或增强独立写入完成信号 | 多次 stat + Pillow verify 是兼容性降级；不能严格证明其他进程已关闭 fd |
| OPEN-07 | P2 代码质量 | 可对 AStore 多数纯读事务改为普通 read transaction，减少 SQLite 写锁争用；模块按团队规范拆得更细 | 本版用 BEGIN IMMEDIATE 简化事务一致性，可能影响极高并发情况下的读吞吐 |
| OPEN-08 | P2 扩展性 | 归档 zstd、递归 input、未来可研究 adaptive、长轮询、对象存储、多消费者 | 不在本次 MVP 范围；不得为这些优化削弱现有 N1–N9 |

## 五、推荐上线判定

**本包可以作为工程 MVP 的可运行实现和验收基线，不应未经验证就部署为无人值守的唯一数据存储系统。** 对正常通道而言，代码已建立源图和归档两次删除前的硬门槛，并通过进程崩溃故障注入测试；但在目标 Linux/Windows、真实 age/SSH、实际生产者写入方式和大包负载条件下完成 OPEN-01 至 OPEN-03 后，方可给出正式生产上线结论。
