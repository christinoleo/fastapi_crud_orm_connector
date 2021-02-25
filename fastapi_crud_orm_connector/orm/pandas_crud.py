from typing import Dict, List

import pandas as pd
from fastapi import HTTPException

from fastapi_crud_orm_connector.orm.crud import Crud, GetAllResponse, DataSort, DataSortType
from fastapi_crud_orm_connector.utils.pydantic_schema import SchemaBase


class PandasCrud(Crud):
    def __init__(self, df: pd.DataFrame, schema: SchemaBase, column_id=None):
        super().__init__(schema)
        self.df = df
        self.column_id = column_id if column_id else df.columns[0]

    def get(self, entry_id):
        ret = self.df.iloc[entry_id]
        if ret is None:
            raise HTTPException(status_code=404, detail="not found")
        return self.schema.instance(**ret.dropna().to_dict())

    def get_all(self, offset: int = 0,
                limit: int = 25,
                data_filter: Dict = None,
                data_sort: DataSort = None,
                data_fields: List = None):
        ret = self.df

        if data_filter:
            for k, v in data_filter.items():
                ret = ret[ret[k] == v]

        if data_sort:
            ret = ret.sort_values(by=data_sort.field, ascending=data_sort.type != DataSortType.ASC)

        if data_fields:
            ret = ret[data_fields]

        total_count = len(ret)
        ret = ret.iloc[offset:min(offset + limit, len(ret))]
        ret = [self.schema.instance(**v.dropna().to_dict()) for k, v in ret.iterrows()]
        return GetAllResponse(list=ret, count=total_count)

    # def create(self, entry):
    #     db_entry = self.model(**entry.dict())
    #     self.db.add(db_entry)
    #     self.db.commit()
    #     self.db.refresh(db_entry)
    #     return self.schema.instance.from_orm(db_entry)
    #
    # def get_or_create(self, entry, data_filter: Dict = None):
    #     ret = self.db.query(self.model).filter(data_filter).first()
    #     if not ret:
    #         ret = self.create(entry)
    #     return ret
    #
    # def delete(self, entry_id: int):
    #     entry = self.get(entry_id)
    #     if not entry:
    #         raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Not found")
    #     self.db.delete(entry)
    #     self.db.commit()
    #
    # def edit(self, entry_id: int, entry, commit=True):
    #     db_entry = self.get(entry_id)
    #     if not db_entry:
    #         raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Not found")
    #     update_data = entry.dict(exclude_unset=True)
    #
    #     for key, value in update_data.items():
    #         setattr(db_entry, key, value)
    #
    #     if commit:
    #         self.db.add(db_entry)
    #         self.db.commit()
    #         self.db.refresh(db_entry)
    #     return self.schema.instance.from_orm(db_entry)

    def count(self, data_filter: Dict = None):
        ret = self.df

        if data_filter:
            for k, v in data_filter.items():
                ret = ret[ret[k] == v]

        total_count = len(ret)
        return total_count
