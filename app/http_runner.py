from __future__ import annotations

import uvicorn
from pathlib import Path

from app.config import default_settings
from app.main import create_app


WEBSOCKET_MAX_MESSAGE_BYTES = 16 * 1024
WEBSOCKET_MAX_QUEUE_MESSAGES = 1


def main() -> None:
    settings = default_settings(Path.cwd())
    app = create_app(settings=settings, embed_smtp=True)
    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        reload=False,
        limit_concurrency=getattr(settings, "http_concurrency_limit", 1000),
        ws_max_size=WEBSOCKET_MAX_MESSAGE_BYTES,
        ws_max_queue=WEBSOCKET_MAX_QUEUE_MESSAGES,
    )


if __name__ == "__main__":
    main()
