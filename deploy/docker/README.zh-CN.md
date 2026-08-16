# Docker 部署

Docker Compose 是 Rapid Inbox 的主要生产部署方式。受支持的启动器会构建一个
非 root 应用容器，并在同一容器内运行一个 FastAPI HTTP 进程和一个 C++
`rapid-inbox-ingestd` 进程。

两个进程必须共享 PID 命名空间和同一个数据卷。跨进程维护协议会持久化
ingestd 的 OS PID，并由 Python 探测该 PID；将 HTTP 与 SMTP 拆到不同容器，
或者扩容该服务，都会产生错误的存活判断。

## 前置条件

- Docker Engine 与 Compose v2 插件（命令为 `docker compose`）
- Docker 卷位于本机 POSIX 文件系统
- 主机 TCP 8000、25 端口可用，或者按下文修改端口
- HTTP 管理面暴露到外部前配置可信的 HTTPS 反向代理

不要把数据卷放到 NFS、CIFS/SMB、FUSE、对象存储挂载，或任何无法可靠提供
POSIX 文件锁、原子重命名和 SQLite WAL 语义的文件系统。一个数据卷只能运行
一个 `app` 副本，不要执行 `docker compose up --scale app=...`。

## 首次部署

在仓库根目录执行：

```bash
./docker-deploy.sh
```

这一条命令会：

1. 创建 `.rapid-inbox-docker/rapid-inbox.env`，随机生成初始管理员密码、API
   游标密钥和指标密钥。
2. 将目录权限设为 `0700`，文件权限设为 `0600`。
3. 先完成镜像构建，再停止已有部署。
4. 先启动 HTTP；应用自身的生命周期会初始化并迁移 SQLite、执行启动恢复，
   readiness 通过后才启动 ingestd。
5. 启动 ingestd，并等待 HTTP 与 SMTP 协议联合健康检查通过。

首次成功部署后，生成的密码只显示一次。之后可在本机重新查看：

```bash
./docker-deploy.sh credentials
```

仓库中的开发用 `.env` 不会复制进镜像，启动器也不会把它作为容器运行配置。

## 端口与配置

默认值如下：

| 用途 | 主机监听 | 容器监听 |
| --- | --- | --- |
| HTTP/管理后台/API | `127.0.0.1:8000` | `0.0.0.0:8000` |
| SMTP 收信 | `0.0.0.0:25` | `0.0.0.0:2525` |

修改 `.rapid-inbox-docker/rapid-inbox.env` 可调整主机监听：

```dotenv
HTTP_PUBLISHED_ADDRESS=127.0.0.1
HTTP_PUBLISHED_PORT=8000
SMTP_PUBLISHED_ADDRESS=0.0.0.0
SMTP_PUBLISHED_PORT=25
```

必须保护该文件。也可以把 `.env.example` 中其余应用或 ingestd 参数加入此文件。
Compose 会固定 `STORAGE_ROOT`、`DATABASE_PATH` 及容器内部监听地址和端口，确保
两个进程始终安全地共享 `/var/lib/rapid-inbox`。

Rapid Inbox 不终止 HTTP TLS；Docker 也不会配置反向代理、防火墙、NAT、SMTP
中继或 DNS。除非可信代理拓扑明确要求，否则 HTTP 应保持仅回环地址可访问。
如果反向代理发送 forwarded headers，只把代理的确切容器 IP 或最小稳定网段写入
私密配置，例如 `FORWARDED_ALLOW_IPS=172.18.0.5`。绝不能设置
`FORWARDED_ALLOW_IPS=*`，否则不可信客户端可能伪造 scheme 或客户端地址元数据。
域名 MX/A 记录以及公网 TCP 25 到 SMTP 发布端口的路由需要单独配置。

容器健康检查每 15 秒建立一次本机 SMTP 会话，只发送 `EHLO`、`NOOP`、`QUIT`，
不会提交邮件。这会为 `SMTP_CONNECTION_RATE_LIMIT_COUNT` 增加约每分钟 4 次回环
连接。如果自定义该限制，必须为健康探测和其他本机操作保留容量，否则 Docker 会
正确地把 SMTP 标记为不健康。

## 日常操作

```bash
./docker-deploy.sh status
./docker-deploy.sh logs
./docker-deploy.sh logs app
./docker-deploy.sh update
./docker-deploy.sh data-volume
./docker-deploy.sh down
```

`update` 会先拉取当前基础镜像，并在旧容器仍运行时构建新镜像；构建成功后才
优雅停止两个写入进程。新容器完成 HTTP schema 初始化与恢复后才启动 SMTP。
构建失败时旧容器继续运行；启动失败时命名卷会保留，失败容器也会留下供查看
日志。

`down` 会删除容器和 Compose 网络，但保留数据。除非明确要永久删除全部数据，
不要运行：

```bash
# 数据丢失：删除持久化数据库以及全部邮件和附件文件。
docker compose --project-name rapid-inbox down -v
```

## 持久化、备份与恢复

应用数据位于 Docker 命名卷。默认 project 对应
`rapid-inbox_rapid-inbox-data`；自定义 `RAPID_INBOX_COMPOSE_PROJECT` 会改变卷名前缀。
不要猜测卷名，应先解析实际名称：

```bash
DATA_VOLUME="$(./docker-deploy.sh data-volume)"
```

主机上的私密配置单独位于 `.rapid-inbox-docker/rapid-inbox.env`，两者都需要备份。

执行一致的离线备份：

```bash
./docker-deploy.sh down
DATA_VOLUME="$(./docker-deploy.sh data-volume)"
docker volume inspect "$DATA_VOLUME" >/dev/null
install -d -m 0700 .rapid-inbox-backups
install -m 0600 \
  .rapid-inbox-docker/rapid-inbox.env \
  .rapid-inbox-backups/rapid-inbox.env
docker run --rm \
  -v "$DATA_VOLUME:/source:ro" \
  -v "$PWD/.rapid-inbox-backups:/backup" \
  alpine:3.22 \
  sh -c 'umask 077; cd /source && tar -czf /backup/rapid-inbox-data.tar.gz .'
./docker-deploy.sh
```

停止容器后，SQLite、WAL、邮件文件、manifest 和附件会处于同一应用边界。对于
正在运行的 WAL 数据库，只复制 `app.db` 不是有效备份。

恢复时使用新的 Compose project 和不存在的新卷，从而保留原卷用于回滚。下例会在
默认部署存在时先将其停止，再使用隔离的 project/config：

```bash
if [ -f .rapid-inbox-docker/rapid-inbox.env ]; then
  ./docker-deploy.sh down
fi
export RAPID_INBOX_COMPOSE_PROJECT=rapid-inbox-restore
export RAPID_INBOX_CONFIG_FILE="$PWD/.rapid-inbox-docker/rapid-inbox-restore.env"
DATA_VOLUME="$(./docker-deploy.sh data-volume)"
if docker volume inspect "$DATA_VOLUME" >/dev/null 2>&1; then
  echo '恢复中止：目标卷已经存在。' >&2
  exit 1
fi
install -d -m 0700 .rapid-inbox-docker
install -m 0600 \
  .rapid-inbox-backups/rapid-inbox.env \
  "$RAPID_INBOX_CONFIG_FILE"
docker volume create "$DATA_VOLUME"
docker run --rm \
  -v "$DATA_VOLUME:/target" \
  -v "$PWD/.rapid-inbox-backups:/backup:ro" \
  alpine:3.22 \
  sh -c 'cd /target && tar -xzf /backup/rapid-inbox-data.tar.gz && chown -R 10001:10001 .'
./docker-deploy.sh
```

不要把备份覆盖解压到任何已存在的卷。必须使用唯一的恢复 project 名；其预期卷
若已存在，文档流程会直接中止。若宿主机端口已被占用，应在启动前修改恢复配置中的
`HTTP_PUBLISHED_PORT` / `SMTP_PUBLISHED_PORT`。清理任何内容前先验证恢复结果；原配置和
原卷不会被改写。`docker volume rm` 是破坏性操作，因此部署脚本不会代为执行。

## 回滚

高风险更新前应记录源码版本，并完成上述离线数据与配置备份。回滚应用代码时，
切换到上一个已审核版本后执行 `./docker-deploy.sh`；脚本仍会先构建，再停止当前
容器。如果新版本做过旧版本不兼容的 schema 迁移，不要让旧代码直接使用已迁移
的卷，而应恢复与旧版本匹配的更新前数据卷和配置备份。
