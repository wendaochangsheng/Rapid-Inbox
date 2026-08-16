## Changes / 改动内容

- 

## Tests / 测试

- [ ] `.venv/bin/pytest`
- [ ] `python3 -m compileall -q app tests`
- [ ] `ctest --test-dir cpp/ingestd/build --output-on-failure` (C++ changes / 涉及 C++)
- [ ] `.venv/bin/pytest tests/test_cpp_ingestd_integration.py` (C++ or shared ingest path; build ingestd first / 涉及 C++ 或共享收件链路，需先构建 ingestd)
- [ ] Documentation-only change; links and commands checked manually / 仅文档改动，已人工检查链接和命令

## Affected areas / 影响范围

- [ ] Database schema / 数据库结构
- [ ] API behavior / API 行为
- [ ] Admin UI / 管理后台页面
- [ ] Public inbox UI / 公开收件箱页面
- [ ] SMTP ingest path / SMTP 接收流程
- [ ] Documentation / 文档

## Notes / 备注

Describe compatibility, migration steps, screenshots, or anything else maintainers should review.

请说明兼容性、迁移步骤、截图或还需要维护者关注的点。
