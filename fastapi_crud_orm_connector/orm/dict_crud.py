from typing import Dict, List, Union

import pandas as pd

from fastapi_crud_orm_connector.orm.pandas_crud import PandasCrud
from fastapi_crud_orm_connector.utils.pydantic_schema import PandasSchema, SchemaBase


class DictCrud(PandasCrud):
    def __init__(self,
                 data: List[Dict],
                 schema: Union[PandasSchema, str],
                 column_id=None,
                 ):
        # df = pd.json_normalize([{'email':'username', 'hashed_password': 'password'}])
        df = pd.json_normalize(data)
        super().__init__(df=df, schema=schema, column_id=column_id)
