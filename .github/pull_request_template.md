## 改动内容

- 

## 测试

- [ ] `.venv/bin/pytest`
- [ ] `python3 -m compileall -q app tests`
- [ ] `ctest --test-dir cpp/ingestd/build --output-on-failure`（涉及 C++）
- [ ] `.venv/bin/pytest tests/test_cpp_ingestd_integration.py`（涉及 C++/共享收件链路，需先构建 ingestd）
- [ ] 仅文档改动，已人工检查链接和命令

## 影响范围

- [ ] 数据库结构
- [ ] API 行为
- [ ] 管理后台页面
- [ ] 公开收件箱页面
- [ ] SMTP 接收流程
- [ ] 文档

## 备注

请说明兼容性、迁移步骤、截图或还需要维护者关注的点。
