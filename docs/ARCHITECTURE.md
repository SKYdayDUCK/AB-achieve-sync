# AAS v1.2 + PATCH-01 — 实现结构与状态不变量

## 决策优先级与阶段范围

原始 v1.2 的单 A、单 B、SSH、两次正常删除、应急弱保证边界不变；PATCH-01 把 `fixed_interval` 改为唯一可注册的默认生产策略，并追加 PASS 证书恢复、B 队列事务与交付不确定窗口处理。本源码按修订后协议实现，不使用原 v1.2 的 `adaptive` 配置。

## A 正常归档

```text
input/ (complete; stable; one directory level)
   │ input_claim.lock 互斥
   ▼
staging/<batch> / A.db: CLAIMING→PACKAGING
   │ snapshot(name,bytes,sha256,dev,ino,mtime_ns)
   ▼
building/<batch>/plaintext.tar.part → crypto.encrypt_file → bundle
   │ manifest.json 固定 payload 明文/密文 SHA、file_index 和 snapshot SHA
   ▼
VerifyGate: 从 bundle 回读 → 解密到私有临时文件 → tar 安全全遍历
   │ 独立校验每个成员数量、大小、SHA、路径；策略假报成功不影响决策
   ▼
verification_certificate.json (PASS) + DB: PUBLISHING (hash binding)
   │ 移除明文临时包；文件持久化；renameat2(NOREPLACE) 原子发布
   ▼
ready/<batch>/bundle+manifest+certificate / DB: SOURCE_CLEANING
   │ 销毁 A 临时审核私钥；每次删源前重新核对归档与证书指纹
   │ staging/<batch>/精准逐图 unlink；fsync 子目录；审计日志
   ▼
READY + ready_event_id (同一 A SQLite 事务)
   │ ... B 完成交付 + 独立 DELIVERED receipt ...
   ▼
授权 ACK + DB 持久墓碑 → ACKED → DELETING → DELETED
```

- `PUBLISHING` 崩溃：凭 DB 绑定与 PASS 证书检查 `building/<id>` 或已发布 `ready/<id>` 后续发布，**不必恢复临时审核私钥**。
- `SOURCE_CLEANING` 崩溃：复核最终加密 payload 字节、manifest SHA 和 PASS 证书。丢失/损坏证明直接转 `VERIFY_RECOVERY_BLOCKED`，原图剩余部分不会再删。
- 若在最后一张源图 `unlink` 后、`SOURCE_CLEANING→READY` 前崩溃，逐项的 `SOURCE_UNLINKED` / `SOURCE_ABSENT_CONFIRMED` 审计使目录已经不在的情况仍可恢复。
- `ACKED/DELETING` 崩溃：持久 ACK 是删除资格；缺失中间某个包内文件可幂等续删；批次目录已删除但 DB 未完成时补写墓碑。绝不会清理没有 ACK 的 READY。
- “文件不可变”主要由发布后的 `chmod(0440)`、内控路径和哈希契约保护；不宣称可抵抗本机 root 恶意篡改。

## B 状态机

```text
SSH hello → list-ready (snapshot_high, event_id) → B sqlite transaction
  ├── UPSERT transfers (immutable payload/manifest identity)
  └── UPDATE discovery_cursor (same transaction)

DISCOVERED / RETRY → RUNNING → download .part (get-range)
 → ciphertext hash → decrypt + plaintext hash → tar image hashes
 → private restoring/ → receipt PREPARED (fsync)
 → output/<producer>/<batch>/ no-overwrite rename
 → receipt DELIVERED (fsync) → ACK_PENDING → SSH ACK → ACK_CONFIRMED

PREPARED+no output+private exists → validate private snapshot → rename → DELIVERED
PREPARED+output exists+verified snapshot → DELIVERED
PREPARED+output absent+private absent → HANDOFF_UNCERTAIN (operator only)
DELIVERED receipt present → resend ACK *without scanning user output*
```

- B SQLite 可由独立 `receipts/*.json` 的 DELIVERED 条目恢复派生索引；手动删除收据和 DB 则失去保证。
- 交付时不能覆盖用户已存在的目标目录；Linux 与 Windows 都采取禁止覆盖的原子目录重命名。
- 用户文件出现在 `output/` 后归用户自由管理；已形成的 DELIVERED 收据不可被它们后续变化撤销。

## READY 分页/游标

1. A 独立 `seq`（批次唯一序号）、`ready_event_id`（每次 READY 重宣告新 ID）。首次分页锁定 `snapshot_high`，后续分页携带相同值。
2. 每页先在 B SQLite `BEGIN IMMEDIATE` 中登记所有待办，再提交 `cursor`，避免在一次崩溃中漏永久事件。`protocol_epoch` 不匹配则终止并需要人工核对，不做暗中重置。
3. 同一不可变 batch 若出现不同 payload/manifest 哈希，整个页事务回滚且中止扫描；已 DELIVERED 不因旧响应被覆盖。
4. 独立的定期全量 READY + `ledger` 审计；不会依赖 seq > cursor 推断是否存在新包。

## PollPolicy 与 RetryPolicy

启动立即扫描，成功全轮分页结束后使用单调时钟设置 `next_due=finished+interval_seconds`。发现新增待办最多再立即复查一次；**下载永不在发现线程内执行**；默认 2 个有界线程。SSH 失败由指数退避决定重试，不登记成功空样本。独立 full READY/ledger 定期对账，无论切换哪种 PollPolicy 都必须保留。

`adaptive_research`/`long_poll` 未注册；配置无法偷偷启用。固定 T 是可复现研究基线，未来新策略只改变下一次 READY 查询时间，不介入 ACK/VerifyGate/两类删除。

## 应急模式

另一授权 key 仅拥有 `emergency-stream` 权限；A 持有与正常 batch claim 共用的 `input_claim.lock`，把未领取完整图移入 `emergency_claim`，发送 `len(header)+JSON(header)+raw file`；B 完整接收、fsync、校验 SHA 后回 `OK(token,sha)`；A 才直接 `unlink` 对应已领取文件。中断时来源可重新进入 input，**允许重复、人工介入，不冒充 normal exactly-once**。

## 容错排除条件

正常通道针对进程退出、网络中断和恢复性 I/O 失败（持久卷可用且状态未被外部破坏）；不保护 A 整盘毁损、机密私钥永久丢失、拥有本地同 UID/root 权限的对抗性进程、用户输出的持续存在、或者原始生产者未遵循完成文件协议导致生成器写入正在被移动的文件。
