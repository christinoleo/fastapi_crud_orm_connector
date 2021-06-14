from enum import Enum
from typing import Dict, List, Type, Any, Union

from pydantic import BaseModel

from fastapi_crud_orm_connector.utils.pydantic_schema import SchemaBase


class DataSortType(str, Enum):
    ASC = "ASC"
    DESC = "DESC"


class DataSort(BaseModel):
    field: str
    type: DataSortType = DataSortType.DESC


class MathOperation(str, Enum):
    sum = "sum"
    count = "count"
    min = "min"
    max = "max"
    mean = "mean"


class DataGroupBy(BaseModel):
    data_fields: List[str]
    operation: MathOperation
    unstack: bool = False


class GetAllResponse(BaseModel):
    list: Any
    count: int


class Crud:
    def __init__(self, schema: SchemaBase = None):
        self.schema = schema

    def use_db(self, db):
        self.db = db
        return self

    def get(self, entry_id: int, convert2schema: Union[bool, Type[BaseModel]] = True):
        raise NotImplemented()

    def get_first(self,
                  data_filter: Dict = None,
                  data_fields: List = None,
                  *,
                  convert2schema: Union[bool, Type[BaseModel]] = True
                  ):
        return self.get_all(0, 1, data_filter=data_filter, data_fields=data_fields, convert2schema=convert2schema).list[0]

    def get_or_create(self, entry, data_filter: Dict = None):
        raise NotImplemented()

    def get_all(self,
                offset: int = 0,
                limit: int = 25,
                data_filter: Dict = None,
                data_sort: DataSort = None,
                data_fields: List = None,
                convert2schema: Union[bool, Type[BaseModel]] = True
                ) -> GetAllResponse:
        raise NotImplemented()

    def create(self, entry):
        raise NotImplemented()

    def delete(self, entry_id: int):
        raise NotImplemented()

    def edit(self, entry_id: int, entry):
        raise NotImplemented()

    def count(self, data_filter: Dict = None) -> int:
        raise NotImplemented()

    def _calculate_schema(self, data, convert2schema: Union[bool, Type[BaseModel]] = True):
        if not convert2schema:
            return data
        elif isinstance(data, list):
            def spread(x, call, **args):
                return [call(r, **args) for r in x]
        else:
            def spread(x, call, **args):
                return call(x, **args)

        if convert2schema is True:
            return spread(data, self.schema.converter, schema_type=None)
        else:
            return spread(data, self.schema.converter, schema_type=convert2schema)
