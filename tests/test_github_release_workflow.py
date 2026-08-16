from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "release-ingestd.yml"


def test_release_workflow_builds_and_publishes_ingestd_binary() -> None:
    content = WORKFLOW.read_text(encoding="utf-8")

    assert "actions/checkout@v7" in content
    assert "cmake -S cpp/ingestd -B cpp/ingestd/build" in content
    assert "ctest --test-dir cpp/ingestd/build --output-on-failure" in content
    assert "python -m pytest -q tests/test_cpp_ingestd_integration.py" in content
    assert "actions/setup-python@v7" in content
    assert "rapid-inbox-ingestd-linux-x86_64.tar.gz" in content
    assert "actions/upload-artifact@v7" in content
    assert "actions/download-artifact@v8" in content
    assert "softprops/action-gh-release@v3" in content
    assert "startsWith(github.ref, 'refs/tags/')" in content
