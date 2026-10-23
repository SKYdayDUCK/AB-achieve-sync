# 本地 Code Review 与试运行前验证记录（2026-10-22）

原包自带报告（33 passed）仅在 Linux 容器内得出。本次在 **Windows 原生（B 目标平台）+ 真实 age + 真实子进程 SSH 传输层（缺 sshd）** 下复测。

## 已修复
| # | 级别 | 问题 | 修复 |
|---|---|---|---|
| 1 | 阻断 | Windows 上 `os.fsync` 作用于只读句柄 → `Bad file descriptor`；B 的解密/交付（none 与 age）永远失败（初测 25/34 失败） | crypto/core.py 四处改 `r+b` |
| 2 | 中 | 发布文件 `chmod 0o440` 在 Windows 上变只读属性，unlink 失败（仅影响在 Windows 跑 A 的测试） | a/core.py 非 nt 才 chmod |
| 3 | 低 | `with sqlite3.connect()` 只提交不关闭，连接泄漏（Windows 下文件被占用） | A/B store 显式 close |
| 4 | 中 | ACK 在 A 端同步重算整包哈希后再删包，而 ACK 复用 30s 查询超时；GiB 级包会超时 | ACK 超时 1800s（transport/ssh.py） |
| 5 | 中 | 下载哈希不符时损坏的 `.bundle.part` 保留，retry-held 后永远重复同一错误 | 不符即删除 part |
| 6 | 中 | 应急流每发一个文件都对 input 全量 `Image.verify`（积压 N 张 → O(N²)）；正常 claim 也每轮全量验证 | `_eligible_names(limit=)`，应急 1、正常 max_files |
| 7 | 低 | 崩溃后遗留的 A 临时审核私钥不清理 | clean_sources 开头删除 |
| 8 | 中 | **ACK 并发重复**：交付后 B 把批次置 `ACK_PENDING`（可被领取）而第一个 worker 仍在发 ACK，第二个 worker 领走同批并发再发 ACK（真实服务器审计日志中观察到）；A 幂等所以未损坏，但大包会并发清理 | `_complete_delivered/_ack` 期间保持 RUNNING；新增回归测试 |
| 9 | 高(运维) | 本机 DNS 的 AAAA 查询约一半概率卡 6–13 秒，使每次 SSH 调用 1.3s→13s，"10 秒轮询"实际 15–38 秒 | 新增 `remote.address_family: inet`（实测 12/12 次 1.2–1.4s） |

## 未改、需知晓
- 工作目录有 Windows 保留名/特殊字符/非图片后缀/超 `max_batch_bytes` 的文件会被静默跳过，无告警。
- 每个 8MiB 块一次 SSH 连接（Windows OpenSSH 无 ControlMaster），高延迟链路吞吐受限；可调大 `chunk_bytes`（上限 16MiB）。
- 应急 `select.select` 作用于管道，仅 Linux 可用（A 本就是 Linux）。
- `none` 模式的 crypto-probe 明文回传，仅为连通性检查。

## 验证结果
- Windows 原生：33 passed / 1 skipped（age 缺失时）；放入真实 age.exe 后 age 用例 4 passed。
- 真实 age 端到端：错误私钥 → B `MANUAL_HOLD`、不 ACK、A 保持 READY；正确私钥 → 交付 + ACK + A DELETED；A 审核私钥目录为空。
- `scripts/local_ssh_shim_smoke.py`：真实 `SSHTransport` + 每次调用独立 `aasctl-shell` 子进程，2 批 3 图全部 DELETED/ACK_CONFIRMED；consumer 角色调用 `emergency-stream` 被拒。
- **未验证**：Linux 上 A 的全套测试（本机 WSL 磁盘路径丢失）、真实 sshd/受限 authorized_keys、`renameat2`、应急流、4GiB 大包、72h。这些在试运行时于服务器上做。
