from typing import Dict, List

import pandas as pd

from fastapi_crud_orm_connector.utils.database_session import DatabaseSession


class PandasSession(DatabaseSession):
    def __init__(self, database: pd.DataFrame):
        self.database = database

    def get_db(self):
        yield self.database


class DictSession(PandasSession):
    def __init__(self, database: List[Dict]):
        super().__init__(pd.json_normalize(database))
