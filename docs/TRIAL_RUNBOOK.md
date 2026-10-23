# 试运行手册（A = 云 GPU 服务器，B = 本机 Windows）

目录约定：代码目录 `<repo>`；B 运行时目录 `<runtime>`（放 venv、age、`b.yaml`、`aas-b.cmd`）；
密钥 `~/.ssh/id_ed25519_aas{,_emergency}`、`~/.config/aas/b_{primary,backup}.{key,pub}`（B 私钥只在本机，A 只拿 `.pub`）。

## 部署注意点（云容器场景）
1. 实例每次开机 **host/port 会变**：改 `runtime\b\b.yaml` 的 `remote.host/port`，并确认 `known_hosts` 里有 `[host]:port` 条目（首次用普通 `ssh` 核对指纹后接受，AAS 自身一律 `StrictHostKeyChecking=yes`）。
2. 容器通常无 systemd：A 用 `deploy/aas-a-daemon.sh` 的 nohup+pidfile；**实例重启后需重新 start**。
3. `input_dir` 与 `BASE`（AAS 根）必须同一文件系统（A 用 rename 领取）；放数据盘（如 `/root/autodl-tmp`），系统盘通常很小。
4. 服务器 root 账号同时有平台登录密钥：AAS 只追加两行带 `restrict,command=` 的受限公钥（标记 `# aas-managed`），不改动其他密钥。
5. A 打包峰值占用约 3× 批大小（tar + bundle + 回读解密）+ `critical_free_bytes`，容量不足会拒绝打包而不会删未 ACK 的包。
6. **半截文件保护（A 端目录监控）**：A 永远不打包目录里最新的那张图（`watch.hold_back_newest: 1`，顺序出图的生成器开始写新文件，说明旧文件已写完，即 n+1 规则）。
   超过 `idle_flush_factor`（默认 4）× 近期出图间隔（取最近文件修改时间间隔的中位数，限制在 10–300 秒；学到 3 个间隔之前用上限 300 秒）没有新图或新的 `.tmp`，视为生成结束，把剩下的包括最新那张一起打包。
   前提：生成器是**顺序**写图。若多进程并行写图，较旧的文件也可能仍在写，应把 `hold_back_newest` 调成并行数，或改用 `.tmp`→rename。`aas-a status` 的 `watch` 字段会显示被留住的文件和当前阈值。
7. 生成器请用 `xxx.tmp → rename(xxx.png)`；否则只靠 3 秒 mtime 稳定性 + Pillow verify（JPEG 截断可能漏检）。非图片后缀、含 `:*?"<>|`、Windows 保留名的文件会被**静默跳过**。
8. Windows 长路径：输出路径 = `output\gpu-a-01\<batch_id>\<文件名>`，文件名较长时可能超 260 字符，建议开启 LongPathsEnabled。

## 步骤
1. 启动服务器，把 `host`、`port` 告诉我（或自己填入 b.yaml）。
2. 上传代码并部署 A：`scp -r AB-achieve-sync`、两个 SSH 公钥、两个 age 公钥 → `bash deploy/a_setup.sh <BASE> <INPUT> ...` → `bash deploy/aas-a-daemon.sh <BASE> start`。
3. 向 `INPUT` 上传一张图片（试运行把 `AAS_MAX_WAIT=30`，30 秒后即成批）。
4. B：`runtime\aas-b.cmd crypto-probe` → `runtime\aas-b.cmd once`；检查 `runtime\b\output\gpu-a-01\<batch_id>\` 有图、A 端 `aas-a status` 为 DELETED、`input` 与 `ready` 已清空。
5. 验证隔离：普通 key 执行 `emergency-stream` 应被拒；再测手动应急 `runtime\aas-b.cmd emergency`。
