from deepagents import create_deep_agent

from ..config import AppConfig
from ..utils.logger import get_logger

logger = get_logger(__name__)


class MonitorAgent:
    def __init__(self, config: AppConfig):
        self.config = config

    async def check_status(self, job_name: str) -> dict:
        logger.info("status_check", job=job_name)
        return {"job": job_name, "status": "completed", "timestamp": "..."}
