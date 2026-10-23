# AAS 安全边界与操作前提

## 不可绕过的断言

- A 正常删源前必须是 `SOURCE_CLEANING`、已有 PASS 证书与 final payload/manifest DB 双重哈希绑定；中断后重新计算**最终已发布**两项哈希。**只看加密插件返回 True 是无效证据**。
- A 删除归档前必须有与授权 consumer 匹配的 ACK 墓碑，且满足 `ACKED/DELETING` 状态；不因磁盘告警、下载完成、B 的 PREPARED 或远程请求路径而删除。
- B 正常 ACK 前必须有独立内部 `DELIVERED` JSON 收据；分阶段下载和 PREPARED 都不能触发 ACK。交付后用户自行修改的输出不导致重传或自动替换。
- B Windows/Linux 输出路径拒绝绝对路径/`..`/设备名/尾随点空格/大小写碰撞/软硬链接和 symlink；tar 只按 manifest 里的预期普通文件写出，不调用 `extractall()`。
- `aasctl-shell` 只允许白名单 RPC；正常消费 key 禁止 emergency-stream；应急 key 禁止普通 RPC；两类 key 都应使用 OpenSSH 的 `restrict` 选项。

## 信任边界

- A 进程用户和 SQLite/内部证书文件是**可信的本地状态**；不保护同 UID 恶意程序、root、云平台运维对运行时图片的访问。归档证书只提供意外改动的完整性恢复依据，不是远程可证明执行环境或抗恶意管理员签名。
- A 的 `age_x25519` 最终包加密给 B 主接收人、建议另加 B 备份接收人、临时 A 审核接收人。A 审核后销毁其临时私钥的**逻辑文件**；不承诺对 SSD/快照安全擦除。
- SSH 主机指纹必须由可靠渠道事先核对后固定。不能给 `StrictHostKeyChecking=no`、不能共享紧急 key 给自动正常任务；建议将 A 配置、数据库、SSH 授权和私钥目录权限限制为专用用户。
- `none` 策略仅用于受控环境演示或明确不需要加密的环境。数据源保护、云端快照、介质完全毁损和用户后续删除内容均不在保障范围。

## 真实生产前的安全检查

1. SSH 用两套 Ed25519 密钥；`authorized_keys` 每行限定 `restrict,command=...`，禁止其它非受限登录密钥。
2. B age 私钥远离 A，仅提供对应**公钥**给 A；至少对 B 的恢复密钥完成一份离线备份和解密测试。
3. A/B 日志不输出 age 私钥、图片明文、SSH 凭证；A `state/audit_keys` 不对普通用户开放。
4. 先隔离小批量执行真实 `age`、断点续传、SHA、交付和 ACK；再启用周期服务。
5. 检查生成器确实使用 `.tmp -> fsync -> rename` 完成协议；`mtime` 稳定检测只是兼容性降级，不能证明写入进程已经关闭句柄。
6. `renameat2(NOREPLACE)` 需要 Linux 对该系统调用和所用文件系统的支持；不支持时本版**拒绝覆盖而不是退化为有风险的 rename**。

## 已承认、未完全消除的限制

| 风险 | 明确处理 |
|---|---|
| A 整盘/云实例失效 | 单副本架构不解决，须自行做异地备份或改架构 |
| B 用户在 rename→DELIVERED 的窗口移动目录 | `HANDOFF_UNCERTAIN`，需记录理由的人工操作，不能虚称绝对 exactly-once |
| Windows 目录 metadata fsync 不具备 Python stdlib 同等支持 | 针对普通进程崩溃设计；跨平台突然断电和 72h soak 需在真实目标盘验收 |
| 手动应急 ACK 丢失 | 可能重新送同一图片，采用较弱保证并明确提示 |
| 大包经每片独立 SSH 请求 | 实现简单、可续传；未实现长连接/SSH ControlMaster 性能优化 |
| age 缺失或 B 解密密钥不正确 | 对应操作失败并保留证据，不删未 ACK bundle；需运维修复密钥 |
