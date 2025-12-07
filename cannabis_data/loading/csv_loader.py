from .base_loader import BaseLoader
import pandas as pd
from cannabis_data.common.exceptions import DataLoadError
import os

class CSVLoader(BaseLoader):
    def __init__(self, filepath: str):
        self.filepath = filepath

    def load(self) -> pd.DataFrame:
        if not os.path.exists(self.filepath):
            raise DataLoadError(f"File not found: {self.filepath}")
        df = pd.read_csv(self.filepath)
        if df.empty:
            raise DataLoadError("CSV returned empty DataFrame")
        return df
