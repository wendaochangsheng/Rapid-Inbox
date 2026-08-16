# Rapid Inbox 原生 systemd 部署（次要方式）

Docker 是 Rapid Inbox 的主要部署方式。本目录保留面向 Debian/Ubuntu 系 systemd 主机的原生一键部署方式。

## 支持范围

- Debian 12 及以上，或 Ubuntu 24.04 及以上；兼容衍生发行版还必须提供 Python 3.10+ 与 CMake 3.25+。
- systemd 必须作为系统服务管理器运行。
- 安装器从当前源码检出构建 Python HTTP 控制面和 C++ `rapid-inbox-ingestd` SMTP 数据面。
- 默认 HTTP 仅监听 `127.0.0.1:8000`。安装器不会配置 TLS、反向代理、防火墙、DNS 或 MX。
- SMTP 默认监听 `0.0.0.0:25`，由 systemd 仅授予 `CAP_NET_BIND_SERVICE`。

## 一键安装

在仓库根目录执行：

```bash
sudo bash deploy/system/install.sh install
```

安装器会安装系统依赖、建立专用低权限用户、构建版本化 release、生成私密配置、初始化 SQLite，随后启用 `rapid-inbox.target`。启动验收同时检查 HTTP `/health/ready` 和 SMTP banner/EHLO/NOOP/QUIT；只打开 TCP 端口不算成功。

路径如下：

- 当前版本：`/opt/rapid-inbox/current`
- 配置：`/etc/rapid-inbox/rapid-inbox.env`
- 数据与 SQLite：`/var/lib/rapid-inbox`
- systemd 单元：`rapid-inbox-http.service`、`rapid-inbox-ingestd.service`、`rapid-inbox.target`

初始管理员账号和密码仅在完整安装与协议验收成功后显示一次；API/指标密钥不会输出。所有值同时写入权限为 `0640` 的配置文件。请在首次登录后立即修改管理员密码。需要检查或修改配置时使用 `sudoedit /etc/rapid-inbox/rapid-inbox.env`，然后执行：

```bash
sudo systemctl restart rapid-inbox.target
```

将 HTTP 改为非回环地址前，应先部署受信任的 HTTPS 反向代理。

## 更新、状态与卸载

```bash
sudo bash deploy/system/install.sh update
sudo bash deploy/system/install.sh status
sudo bash deploy/system/install.sh uninstall
```

更新先完成源码复制、Python 依赖安装和 C++ 构建，然后停止旧 HTTP/SMTP writer，使用 SQLite backup API 建立一致备份，仅在无旧 writer 的情况下迁移 schema，再原子切换版本并验收。构建失败不会停服；迁移前失败会保留旧版本。已有数据库且监听器尚未启动时若迁移或 writer smoke 失败，安装器会尝试恢复迁移前数据库、旧 unit/marker/current，并重启旧版本。首次安装没有旧数据库或旧版本可恢复时，失败会禁用并删除新 unit、current 和新 release，但保留已生成的配置与数据目录供修正后重试。

`status` 除 systemd 状态外还会实际检查 HTTP 和 SMTP 协议。`uninstall` 只移除服务单元和 `/opt/rapid-inbox` 下由安装器管理的版本；配置、数据库、邮件文件、备份及服务用户都会保留。安装器不提供自动 purge，以避免误删数据。

## 完整备份与恢复

更新时自动创建的 `backups/*-pre-migration.db` 只保护 SQLite，不包含配置、raw EML、正文、附件或 manifest，不能代替完整备份。完整备份必须在两个服务都停止后同时保存 `/etc/rapid-inbox` 与 `/var/lib/rapid-inbox`，并保留权限和所有权。例如：

```bash
sudo systemctl stop rapid-inbox.target
sudo tar --acls --xattrs --numeric-owner -C / -czf /root/rapid-inbox-full-backup.tar.gz etc/rapid-inbox var/lib/rapid-inbox
sudo systemctl start rapid-inbox.target
sudo bash deploy/system/install.sh status
```

恢复时先停止 target，并把现有配置和数据移动到单独的回滚目录，而不是直接删除；检查备份内容后从 `/` 解包。在目标主机创建 `rapid-inbox` 服务账号后，确认 `/var/lib/rapid-inbox` 归该账号所有、配置为 `root:rapid-inbox` 且不可公开读取。随后从与备份兼容且已审核的源码执行 `install`（或现有托管安装执行 `update`），让安装器在无 writer 时迁移并完成双协议验收。原目录和备份应保留到业务数据抽查完成。

部署专测命令：

```bash
.venv/bin/python -m pytest -q tests/test_system_deployment.py
```
