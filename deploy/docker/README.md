# Docker deployment

Docker Compose is the primary production deployment path for Rapid Inbox. The
supported launcher builds one non-root application container that runs exactly
one FastAPI HTTP process and one C++ `rapid-inbox-ingestd` process.

The processes intentionally share a PID namespace and one data volume. The
cross-process maintenance protocol stores the ingestd OS PID and probes it from
Python, so splitting HTTP and SMTP into separate containers or scaling this
service creates incorrect liveness results.

## Prerequisites

- Docker Engine with the Compose v2 plugin (`docker compose`)
- A local POSIX filesystem for Docker's volume storage
- Host TCP ports 8000 and 25 available, or alternative ports configured below
- A trusted HTTPS reverse proxy before exposing the HTTP management plane

Do not place the data volume on NFS, CIFS/SMB, FUSE, an object-storage mount, or
another filesystem without reliable POSIX locks, atomic rename, and SQLite WAL
semantics. Run exactly one `app` replica for a data volume. Do not use
`docker compose up --scale app=...`.

## First deployment

From the repository root, run:

```bash
./docker-deploy.sh
```

That one command:

1. Creates `.rapid-inbox-docker/rapid-inbox.env` with random bootstrap, API
   cursor, and metrics secrets.
2. Sets the directory to mode `0700` and the file to mode `0600`.
3. Builds the image before stopping any existing deployment.
4. Starts HTTP first. Its normal application lifespan initializes and migrates
   SQLite, performs startup recovery, and becomes ready before ingestd starts.
5. Starts ingestd and waits for a combined HTTP and SMTP protocol healthcheck.

The generated password is printed once after a successful first deployment.
It can be shown again locally with:

```bash
./docker-deploy.sh credentials
```

The repository's developer `.env` is neither copied into the image nor used as
the container runtime configuration by the launcher.

## Ports and configuration

The defaults are:

| Purpose | Host binding | Container binding |
| --- | --- | --- |
| HTTP/admin/API | `127.0.0.1:8000` | `0.0.0.0:8000` |
| SMTP ingest | `0.0.0.0:25` | `0.0.0.0:2525` |

Edit `.rapid-inbox-docker/rapid-inbox.env` to change the host bindings:

```dotenv
HTTP_PUBLISHED_ADDRESS=127.0.0.1
HTTP_PUBLISHED_PORT=8000
SMTP_PUBLISHED_ADDRESS=0.0.0.0
SMTP_PUBLISHED_PORT=25
```

Keep the file private. The remaining application and ingestd settings from
`.env.example` can also be added there. `STORAGE_ROOT`, `DATABASE_PATH`, and the
internal listener addresses and ports are fixed by Compose so both processes
continue to share `/var/lib/rapid-inbox` safely.

Rapid Inbox does not terminate HTTP TLS and Docker does not configure a reverse
proxy, firewall, NAT, SMTP relay, or DNS. Keep HTTP loopback-only unless a
deliberate trusted proxy topology requires another bind. If a reverse proxy
sends forwarded headers, add only its exact container IP or smallest stable
network to the private config, for example `FORWARDED_ALLOW_IPS=172.18.0.5`.
Never use `FORWARDED_ALLOW_IPS=*`; an untrusted client could then spoof scheme or
client-address metadata. Configure the domain's MX/A records and route public
TCP port 25 to the SMTP published port separately.

The container healthcheck opens a local SMTP session every 15 seconds and sends
only `EHLO`, `NOOP`, and `QUIT`; it never submits a message. This contributes
about four loopback connections per minute to `SMTP_CONNECTION_RATE_LIMIT_COUNT`.
If that setting is customized, leave capacity for these probes and other local
operations or Docker will correctly mark SMTP unhealthy.

## Operations

```bash
./docker-deploy.sh status
./docker-deploy.sh logs
./docker-deploy.sh logs app
./docker-deploy.sh update
./docker-deploy.sh data-volume
./docker-deploy.sh down
```

`update` pulls current base images, builds the new image while the old container
is still running, then gracefully stops both writers. The replacement container
finishes HTTP schema initialization and recovery before it starts SMTP. A build
failure leaves the old container running. A startup failure keeps the named
volume and leaves the failed container available for logs.

`down` removes containers and the Compose network but retains data. Do not run
the following command unless permanent data deletion is explicitly intended:

```bash
# DATA LOSS: deletes the persisted database and every stored message/artifact.
docker compose --project-name rapid-inbox down -v
```

## Persistence and backup

Application data is stored in a Docker named volume. For the default project it
is `rapid-inbox_rapid-inbox-data`; a custom `RAPID_INBOX_COMPOSE_PROJECT` changes
the prefix. Resolve the exact name instead of guessing it:

```bash
DATA_VOLUME="$(./docker-deploy.sh data-volume)"
```

The private host configuration is separate at
`.rapid-inbox-docker/rapid-inbox.env`. Back up both.

For a consistent offline backup:

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

Stopping the container ensures SQLite, its WAL, message files, manifests, and
attachments are captured at one application boundary. Copying only `app.db`
from a running WAL database is not a valid backup.

Restore into a new Compose project and a volume that does not exist. This keeps
the retained original volume available for rollback. The example first stops a
default deployment when present, then uses an isolated project/config pair:

```bash
if [ -f .rapid-inbox-docker/rapid-inbox.env ]; then
  ./docker-deploy.sh down
fi
export RAPID_INBOX_COMPOSE_PROJECT=rapid-inbox-restore
export RAPID_INBOX_CONFIG_FILE="$PWD/.rapid-inbox-docker/rapid-inbox-restore.env"
DATA_VOLUME="$(./docker-deploy.sh data-volume)"
if docker volume inspect "$DATA_VOLUME" >/dev/null 2>&1; then
  echo 'Restore aborted: target volume already exists.' >&2
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

Never extract a backup over an existing volume. Use a unique restore project
name; if its expected volume already exists, the documented restore aborts.
Change the restored `HTTP_PUBLISHED_PORT` / `SMTP_PUBLISHED_PORT` before startup
if those host ports are occupied. Validate the restored deployment before any
cleanup: the original configuration and volume remain untouched. The
`docker volume rm` command is destructive and is intentionally not part of the
deployment script.

## Rollback

Before a high-risk update, record the source revision and create the offline
data/config backup above. To roll back application code, check out the previous
reviewed revision and run `./docker-deploy.sh`; this rebuilds before stopping the
current container. If the new revision migrated the schema in a way the older
revision does not support, do not run old code against the migrated volume.
Restore the matching pre-update volume and config backup instead.
