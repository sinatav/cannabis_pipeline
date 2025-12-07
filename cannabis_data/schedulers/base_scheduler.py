from abc import ABC, abstractmethod
from cannabis_data.common.utils import retry
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)

class BaseScheduler(ABC):

    def __init__(self, retries: int = 2, retry_delay_seconds: int = 300):
        self.retries = retries
        self.retry_delay_seconds = retry_delay_seconds

    @abstractmethod
    def run(self, *args, **kwargs):
        """
        Run the pipeline step. Should be idempotent.
        """
        pass

    def _with_retry(self, fn, *args, **kwargs):
        wrapped = retry(times=self.retries, delay_seconds=self.retry_delay_seconds)(fn)
        return wrapped(*args, **kwargs)
