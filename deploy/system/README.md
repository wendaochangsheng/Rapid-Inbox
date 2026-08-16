# Rapid Inbox native systemd deployment (secondary)

Docker is the primary Rapid Inbox deployment path. This directory provides a secondary, one-command native deployment for Debian/Ubuntu-family systemd hosts.

## Support boundary

- Debian 12+ or Ubuntu 24.04+. Compatible derivatives must provide Python 3.10+ and CMake 3.25+.
- systemd must be running as the system service manager.
- The installer builds the Python HTTP control plane and C++ `rapid-inbox-ingestd` SMTP data plane from the current checkout.
- HTTP binds only to `127.0.0.1:8000` by default. The installer does not configure TLS, a reverse proxy, firewall rules, DNS, or MX records.
- SMTP binds to `0.0.0.0:25` by default. systemd grants only `CAP_NET_BIND_SERVICE` for the privileged port.

## Install

Run from the repository root:

```bash
sudo bash deploy/system/install.sh install
```

The installer installs OS dependencies, creates a dedicated unprivileged account, builds a versioned release, generates private configuration, initializes SQLite, and enables `rapid-inbox.target`. Acceptance checks both HTTP `/health/ready` and an SMTP banner/EHLO/NOOP/QUIT conversation; an open TCP port alone is not accepted.

Managed paths:

- Current release: `/opt/rapid-inbox/current`
- Configuration: `/etc/rapid-inbox/rapid-inbox.env`
- Data and SQLite: `/var/lib/rapid-inbox`
- Units: `rapid-inbox-http.service`, `rapid-inbox-ingestd.service`, `rapid-inbox.target`

The initial administrator username and password are printed exactly once, only after installation and protocol acceptance succeed. API/metrics secrets are not printed. All values are also stored in the mode-`0640` configuration file. Change the administrator password at first login. Inspect or edit configuration with `sudoedit /etc/rapid-inbox/rapid-inbox.env`, then apply changes with:

```bash
sudo systemctl restart rapid-inbox.target
```

Put a trusted HTTPS reverse proxy in place before changing HTTP to a non-loopback bind.

## Update, status, and uninstall

```bash
sudo bash deploy/system/install.sh update
sudo bash deploy/system/install.sh status
sudo bash deploy/system/install.sh uninstall
```

An update copies source and completes Python/C++ builds before downtime. It then stops both old writers, creates a consistent backup through SQLite's backup API, runs schema migration without an old writer, atomically switches the release, and performs protocol acceptance. Build failures leave the running version alone. With an existing database, a pre-listener migration or writer-smoke failure triggers a best-effort restoration of the database, old units/marker/current link, and old release. On a first install with no old database or release, failure disables and removes the new units, current link, and release while retaining generated configuration and data directories for a corrected retry.

`status` performs protocol checks in addition to reading systemd state. `uninstall` removes only managed units and releases under `/opt/rapid-inbox`; configuration, database, mail artifacts, backups, and the service account remain. There is intentionally no automatic purge operation.

## Full backup and restore

The automatic `backups/*-pre-migration.db` files protect SQLite only. They do not contain configuration, raw EML, message bodies, attachments, or manifests and are not full backups. For a complete backup, stop both services and preserve `/etc/rapid-inbox` together with `/var/lib/rapid-inbox`, including ownership and modes. For example:

```bash
sudo systemctl stop rapid-inbox.target
sudo tar --acls --xattrs --numeric-owner -C / -czf /root/rapid-inbox-full-backup.tar.gz etc/rapid-inbox var/lib/rapid-inbox
sudo systemctl start rapid-inbox.target
sudo bash deploy/system/install.sh status
```

For restoration, stop the target and move existing configuration/data to a separate rollback location rather than deleting it. Inspect the archive, then extract it from `/`. After the `rapid-inbox` service account exists on the destination, verify that `/var/lib/rapid-inbox` belongs to it and configuration is private `root:rapid-inbox` data. Run `install` from a reviewed, backup-compatible checkout (or `update` on an existing managed installation) so migration occurs without an old writer and both protocols are accepted. Retain the prior directories and archive until business data has been checked.

Run the deployment-specific tests with:

```bash
.venv/bin/python -m pytest -q tests/test_system_deployment.py
```
