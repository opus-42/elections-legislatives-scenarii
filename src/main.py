"""Run application jobs."""
import logging
from data import get_data_job

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


if __name__ == "__main__":
    get_data_job.execute_in_process()
