"""Local entry point: build config/store/scheduler and serve the HTTP API."""

from __future__ import annotations

import uvicorn

from scheduler.api import create_app
from scheduler.config import Config
from scheduler.logging_setup import configure_logging
from scheduler.scheduler import Scheduler
from scheduler.store import JobStore

_config = Config.from_env()
configure_logging()

scheduler = Scheduler(_config, JobStore(_config.store_path))
app = create_app(scheduler)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
