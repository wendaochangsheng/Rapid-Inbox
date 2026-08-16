**English** | [简体中文](CONTRIBUTING.zh-CN.md)

# Contributing Guide

Thank you for helping improve Rapid Inbox. This project aims to remain a small, clear, local-first tool. When contributing, please keep implementations straightforward, behavior testable, and documentation in sync with the code.

## Contents

- [Development environment](#development-environment)
- [Pre-commit checks](#pre-commit-checks)
- [Branches and commits](#branches-and-commits)
- [Pull requests](#pull-requests)
- [Code style](#code-style)
- [Reporting issues](#reporting-issues)

## Development environment

```bash
# Install dependencies
python3 -m venv .venv
.venv/bin/pip install -c constraints-dev.txt -e ".[dev]"

# Prepare environment variables
cp .env.example .env

# Start HTTP + embedded SMTP
.venv/bin/rapid-inbox-http
```

Run the Python tests and the cross-language integration tests when available:

```bash
.venv/bin/pytest

# Run a specific test file
.venv/bin/pytest tests/test_admin_api.py
```

If your changes affect the C++ ingestd, shared schema, SMTP behavior, or cross-process recovery, you must also build and run the C++ and integration tests:

```bash
cmake -S cpp/ingestd -B cpp/ingestd/build
cmake --build cpp/ingestd/build
ctest --test-dir cpp/ingestd/build --output-on-failure
.venv/bin/pytest tests/test_cpp_ingestd_integration.py
```

## Pre-commit checks

Before opening a PR, run at least:

```bash
.venv/bin/pytest
python3 -m compileall -q app tests
```

These commands do not build ingestd or run CTest. If `cpp/ingestd/build/rapid-inbox-ingestd` does not yet exist, the 3 cross-language integration tests collected by the root `pytest` run will be reported as skipped. For changes to C++ or the shared ingestion path, first run the build and CTest commands above, then rerun `tests/test_cpp_ingestd_integration.py`.
If your changes only affect documentation, make sure that links, commands, versions, and filenames remain accurate.

## Branches and commits

- Create a short-lived branch from `main`, such as `fix/api-key-validation` or `docs/readme-refresh`.
- Keep commits focused: a commit should ideally solve one problem or one tightly related set of changes.
- Commit messages may be written in Chinese, or may follow the [Conventional Commits](https://www.conventionalcommits.org/) style.

Common commit prefixes:

| Prefix | Purpose |
| --- | --- |
| `feat:` | Add a feature |
| `fix:` | Fix a defect |
| `docs:` | Adjust documentation |
| `refactor:` | Refactor without changing external behavior |
| `test:` | Change tests only |
| `chore:` | Build, dependency, tooling, and other maintenance work |

> [!IMPORTANT]
> Do not commit `.env`, `storage/`, database files, real secrets from email samples, or personal data.

## Pull requests

A PR description should include:

- **Purpose**: the problem being solved or the goal being achieved.
- **Key implementation details**: the main design decisions behind the change.
- **Test results**: which tests were run and whether they all passed.
- **Compatibility or migration impact**: whether the database schema, API, or configuration changes.
- **Screenshots or recordings**: when the change affects the UI.

If you plan a large feature, data-structure change, or behavioral change, please open an Issue to discuss the direction first. This helps avoid completing work whose goals do not align with the project.

## Code style

- Follow the existing module boundaries and function style where possible.
- Cover business logic with tests, especially authentication, authorization, data cleanup, and recovery flows.
- Update the README, related documentation, or template text whenever external behavior changes.
- Make error handling explicit where possible; do not swallow exceptions that can affect data consistency.

## Reporting issues

When opening an Issue, please provide as much of the following as possible:

- Version or commit hash
- Python version
- Operating system
- Startup method (`rapid-inbox-http` / `rapid-inbox-smtp` / `uvicorn`)
- Steps to reproduce
- Expected and actual results
- Relevant logs or screenshots

> [!WARNING]
> Redact keys, email content, real domain names, IP addresses, and other sensitive information first. Do not report security vulnerabilities through a public Issue; see [SECURITY.md](SECURITY.md).
