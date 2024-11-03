import logging

import consul
import healthcheck

from cdcron import cdcron

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("main")

logger.info(f"app started")

healthcheck.HealthCheckServer()
consul = consul.Consul()

while True:
    while consul.is_leader:
        cdcron()
