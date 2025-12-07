from abc import ABC, abstractmethod
import pandas as pd

class BaseLoader(ABC):
    """Abstract loader that returns a pandas DataFrame."""

    @abstractmethod
    def load(self) -> pd.DataFrame:
        pass