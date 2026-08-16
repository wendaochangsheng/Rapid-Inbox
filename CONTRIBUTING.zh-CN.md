[English](CONTRIBUTING.md) | **简体中文**

# 贡献指南

感谢你愿意改进 Rapid Inbox。这个项目偏向小而清晰的本地优先工具，贡献时请尽量保持实现直接、行为可测试、文档和代码同步。

## 目录

- [开发环境](#开发环境)
- [提交前检查](#提交前检查)
- [分支与提交](#分支与提交)
- [Pull Request](#pull-request)
- [代码风格](#代码风格)
- [报告问题](#报告问题)

## 开发环境

```bash
# 安装依赖
python3 -m venv .venv
.venv/bin/pip install -c constraints-dev.txt -e ".[dev]"

# 准备环境变量
cp .env.example .env

# 启动 HTTP + 内嵌 SMTP
.venv/bin/rapid-inbox-http
```

运行 Python 与可用时的跨语言集成测试：

```bash
.venv/bin/pytest

# 指定测试文件
.venv/bin/pytest tests/test_admin_api.py
```

若改动涉及 C++ ingestd、共享 schema、SMTP 行为或跨进程恢复，还需要构建并运行 C++ 与集成测试：

```bash
cmake -S cpp/ingestd -B cpp/ingestd/build
cmake --build cpp/ingestd/build
ctest --test-dir cpp/ingestd/build --output-on-failure
.venv/bin/pytest tests/test_cpp_ingestd_integration.py
```

## 提交前检查

请在提交 PR 前至少完成：

```bash
.venv/bin/pytest
python3 -m compileall -q app tests
```

这组命令不会构建 ingestd，也不会执行 CTest；若 `cpp/ingestd/build/rapid-inbox-ingestd` 尚不存在，
根 `pytest` 收集到的 3 个跨语言集成测试会显示为 skipped。涉及 C++ 或共享收件链路时，应先执行
上方构建/CTest，再重新运行 `tests/test_cpp_ingestd_integration.py`。
如果改动只涉及文档，请确保链接、命令、版本和文件名仍然准确。

## 分支与提交

- 从 `main` 拉出短分支，例如 `fix/api-key-validation` 或 `docs/readme-refresh`
- 提交保持聚焦：一个提交最好只解决一个问题或一组紧密相关的改动
- 提交信息可以使用中文，也可以使用 [Conventional Commits](https://www.conventionalcommits.org/) 风格

常用提交前缀示例：

| 前缀 | 用途 |
| --- | --- |
| `feat:` | 新增功能 |
| `fix:` | 修复缺陷 |
| `docs:` | 文档调整 |
| `refactor:` | 不改变外部行为的重构 |
| `test:` | 仅涉及测试 |
| `chore:` | 构建、依赖、工具链等杂项 |

> [!IMPORTANT]
> 不要提交 `.env`、`storage/`、数据库文件、邮件样本中的真实密钥或个人数据。

## Pull Request

PR 描述建议包含：

- **改动目的**：解决什么问题或达成什么目标
- **主要实现点**：关键改动的设计思路
- **测试结果**：哪些测试运行过，是否全部通过
- **兼容性或迁移影响**：数据库结构、API、配置是否有变更
- **截图或录屏**：若改动涉及页面

如果你准备做较大的功能、数据结构调整或行为变更，请先开 Issue 讨论方向，避免做完后发现目标不一致。

## 代码风格

- 优先沿用现有模块边界和函数风格
- 业务逻辑要有测试覆盖，尤其是鉴权、权限、数据清理和恢复流程
- 对外行为变更需要同步更新 README、相关文档或模板文案
- 错误处理尽量明确，避免吞掉会影响数据一致性的异常

## 报告问题

提交 Issue 时请尽量提供：

- 版本或提交哈希
- Python 版本
- 操作系统
- 启动方式（`rapid-inbox-http` / `rapid-inbox-smtp` / `uvicorn`）
- 复现步骤
- 期望结果和实际结果
- 相关日志或截图

> [!WARNING]
> 涉及密钥、邮件内容、真实域名、IP 地址或其他敏感信息时，请先脱敏。安全漏洞请不要通过公开 Issue 报告，参考 [SECURITY.zh-CN.md](SECURITY.zh-CN.md)。
