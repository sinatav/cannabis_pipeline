from sqlalchemy import create_engine, text
import pandas as pd
from cannabis_data.config import DB_URI
from cannabis_data.common.exceptions import DBError

class DBManager:
    def __init__(self, uri: str = None):
        self.uri = uri or DB_URI
        self.engine = create_engine(self.uri, connect_args={"check_same_thread": False} if "sqlite" in self.uri else {})

    def write_df(self, df: pd.DataFrame, table_name: str, if_exists="append"):
        try:
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
        except Exception as e:
            raise DBError(str(e))

    def read_sql(self, query: str) -> pd.DataFrame:
        try:
            with self.engine.connect() as conn:
                return pd.read_sql_query(text(query), conn)
        except Exception as e:
            raise DBError(str(e))

    def execute(self, statement: str):
        try:
            with self.engine.connect() as conn:
                conn.execute(text(statement))
        except Exception as e:
            raise DBError(str(e))
