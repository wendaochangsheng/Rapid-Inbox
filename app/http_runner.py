from __future__ import annotations

import uvicorn
from pathlib import Path

from app.config import default_settings
from app.main import create_app


def main() -> None:
    settings = default_settings(Path.cwd())
    app = create_app(settings=settings, embed_smtp=True)
    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        reload=False,
        limit_concurrency=getattr(settings, "http_concurrency_limit", 1000),
    )


if __name__ == "__main__":
    main()
