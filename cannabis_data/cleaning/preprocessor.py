import pandas as pd
from cannabis_data.common.utils import retry
from cannabis_data.common.exceptions import PreprocessError

class Preprocessor:
    """
    Each method is idempotent and returns a DataFrame.
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    @retry(times=2, delay_seconds=2, exceptions=(Exception,))
    def standardize_columns(self):
        # normalizing columns lowercase, strip, replace spaces
        rename_map = {c: c.strip().lower().replace(" ", "_") for c in self.df.columns}
        self.df.rename(columns=rename_map, inplace=True)
        return self.df

    def ensure_types(self):
        # best-effort conversions
        if "date" in self.df.columns:
            self.df["date"] = pd.to_datetime(self.df["date"], errors="coerce")
        if "sales" in self.df.columns:
            self.df["sales"] = pd.to_numeric(self.df["sales"], errors="coerce").fillna(0.0)
        return self.df

    def fill_missing_essential(self):
        # drop rows missing essential fields (date, store or sku)
        essentials = []
        if "date" in self.df.columns:
            essentials.append("date")
        if "store_id" in self.df.columns:
            essentials.append("store_id")
        if "sku" in self.df.columns:
            essentials.append("sku")
        if essentials:
            self.df.dropna(subset=essentials, inplace=True)
        return self.df

    def add_derived_columns(self):
        # creating derived features year, month, revenue per item.
        if "date" in self.df.columns:
            self.df["year"] = self.df["date"].dt.year
            self.df["month"] = self.df["date"].dt.to_period("M")
        if "quantity" in self.df.columns and "price" in self.df.columns:
            self.df["sales"] = self.df["quantity"] * self.df["price"]
        return self.df

    def run_all(self):
        try:
            self.standardize_columns()
            self.ensure_types()
            self.fill_missing_essential()
            self.add_derived_columns()

            return self.df
        except Exception as e:
            raise PreprocessError(str(e))
