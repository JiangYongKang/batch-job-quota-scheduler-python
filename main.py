"""Local entrypoint: python main.py"""
import os

import uvicorn

from scheduler.app import create_app
from scheduler.config import SchedulerConfig
from scheduler.logging_setup import configure_logging

if __name__ == "__main__":
    configure_logging(os.environ.get("SCHEDULER_LOG_LEVEL", "INFO"))
    config = SchedulerConfig.from_env()
    uvicorn.run(create_app(config), host="127.0.0.1", port=8000)
