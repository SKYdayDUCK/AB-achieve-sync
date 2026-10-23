# AAS SSH 协议 v1 — allow-list 与稳定身份

## SSH 限制

- 仅由 B 主动连接 A；生产公钥绑定 SSH 账户及服务端真实主机公钥；严格 `StrictHostKeyChecking=yes`、`BatchMode=yes`。
- A OpenSSH `authorized_keys` 使用 `restrict,command="...aasctl-shell --role consumer"`；应急使用**另一 SSH 公钥**及 `--role emergency`。
- `SSH_ORIGINAL_COMMAND` 必须是 `aasctl <允许的指令和 flag>`；`shlex.split` 后经 `argparse` 精确解析；没有命令 shell 运行、任意文件路径参数或不受限制的 A 命令执行权限。
- JSON 只走 stdout；错误和服务端日志只走 stderr。`get` 和 `emergency-stream` stdout 为二进制，禁止混入日志。

## 可用只读命令

| 命令 | 输出 | 边界 |
|---|---|---|
| `aasctl hello` | `{protocol_version,producer_id,protocol_epoch,last_ready_event_id,...}` | 首次连接先验证源身份/版本 |
| `aasctl list-ready --after-event-id E --page-size 100 [--snapshot-high H]` | `{producer_id,protocol_epoch,snapshot_high,items,next_event_id,has_more}` | 只列举当前 READY，不充当完整历史日志 |
| `aasctl info --batch-id ID` | 最终 manifest JSON | 只服务 READY，对清单 SHA 作 DB 复核 |
| `aasctl get --batch-id ID --offset O --length L` | 恰好 L 字节 raw payload | 只服务 READY，1 ≤ L ≤ 16 MiB，范围不能越界 |
| `aasctl ledger --since-seq N` | `{items:[{batch_id,seq,state,ready_event_id,...}]}` | 最多 1000 条/页，给独立对账 |
| `aasctl status` | A 容量/状态 | 不管理 B 图库 |
| `aasctl crypto-probe` | nonce 加密挑战+nonce SHA | 不代替真实包解密校验 |

`batch_id` / `producer_id` / `consumer_id` 只接受受限 ID 字符集。客户端不能提交 `ready/<id>` 的任意绝对路径，也不能直接请求 `unlink`。

## ACK 命令

```text
aasctl ack --json '{"producer_id":"gpu-a-01","consumer_id":"desktop-b-01", "batch_id":"...","seq":125,"payload_sha256":"<64hex>","file_count":100,"committed_bytes":601002345,"receipt_version":1}'
```

- `committed_bytes` = **明文图片原始总字节数**，不是密文 bundle 字节数。
- A 核实当前认证 consumer 允许的 `consumer_id`，并逐字段与 A DB 匹配；不同消费者、不同 batch ID/seq/sha/count/bytes 一律拒绝。
- 只有 B 内部 DELIVERED 收据持久化后才调用 ACK；A 在同一事务保存 ACK 墓碑与 `ACKED`。重发返回 `already_acked` / `already_deleted`。
- B 返回的 API `accepted` 是“已持久接受”，归档删除动作的最终完成可由后台恢复工作继续完成；不可把 ACK 当作物理介质已擦除。

## 加密字段与密钥轮换

`manifest.crypto` 为 `scheme`, `version`, `recipient_key_ids`（公钥指纹，不是私钥）。age 的 A 临时审核 identity **只存在 A 私有 `state/audit_keys`**，不会出现在 B 可获取的 manifest、certificate 或 ready 目录。密钥更换需确保 B 新旧接收私钥可解历史包，完成 `crypto-probe`，再恢复正常自动传输；更换 A `protocol_epoch` 需要人工审核。
