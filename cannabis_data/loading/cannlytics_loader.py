from .base_loader import BaseLoader
import pandas as pd
from cannabis_data.config import CANNLYTICS_DATASET
from cannabis_data.common.exceptions import DataLoadError

try:
    from cannlytics.data.opendata import OpenData
    _HAS_CANNLYTICS = True
except Exception:
    _HAS_CANNLYTICS = False


class CannlyticsLoader(BaseLoader):

    def __init__(self, dataset_name: str = None):
        """
        Args:
            dataset_name: Name of dataset to pull (default comes from config)
        """
        self.dataset_name = dataset_name or CANNLYTICS_DATASET
        self.opendata = OpenData() if _HAS_CANNLYTICS else None

    def load(self) -> pd.DataFrame:
        if not _HAS_CANNLYTICS:
            raise DataLoadError(
                "Cannlytics package not installed. "
                "Install it via: pip install cannlytics"
            )

        if self.dataset_name.lower() == "wa_cannabis_sales":
            df = self.opendata.get_sales_data(state="WA")
        else:
            raise DataLoadError(
                f"Dataset '{self.dataset_name}' not supported in OpenData loader"
            )

        if df is None or df.empty:
            raise DataLoadError(f"No data returned for dataset '{self.dataset_name}'")

        return df
