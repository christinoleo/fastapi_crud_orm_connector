from typing import Dict, List, Union, Type

import pandas as pd
from fastapi import HTTPException
from pydantic import BaseModel
from starlette import status

from fastapi_crud_orm_connector.orm.crud import Crud, GetAllResponse, DataSort, DataSortType, DataGroupBy, MathOperation
from fastapi_crud_orm_connector.utils.pydantic_schema import pd2pydantic, PandasSchema


class PandasCrud(Crud):
    def __init__(self,
                 schema: Union[PandasSchema, str],
                 df: pd.DataFrame = None,
                 column_id: Union[str, bool] = 'id',
                 file_path: str = None,
                 ):
        if file_path is None and df is None:
            raise Exception('Need either df or file_path')
        if file_path is not None:
            df = pd.read_csv(file_path, index_col=column_id)

        super().__init__(pd2pydantic(schema, df, column_id=column_id) if isinstance(schema, str) else schema)
        super().use_db(df)
        self.column_id = column_id if column_id is not None and column_id is not False else df.index.name
        self.file_path = file_path
        self.df = df

    def get(self, entry_id, convert2schema: Union[bool, Type[BaseModel]] = True):
        ret = self.df.loc[[entry_id], :].reset_index()
        if ret is None:
            raise HTTPException(status_code=404, detail="not found")
        return self._calculate_schema(ret, convert2schema)[0]

    def get_all(self, offset: int = 0,
                limit: int = 25,
                data_filter: Dict = None,
                data_sort: DataSort = None,
                data_fields: List = None,
                data_group_by: DataGroupBy = None,
                data_parse: Dict = None,
                *,
                convert2schema: Union[bool, Type[BaseModel]] = True
                ) -> GetAllResponse:
        ret = self.df
        ret = ret.reset_index()
        if self.column_id is not None:
            ret['id'] = ret[self.column_id]

        if data_filter is not None:
            for k, v in data_filter.items():
                if isinstance(v, list):
                    ret = ret[ret[k].isin(v)]
                elif isinstance(v, str):
                    ret = ret[ret[k].astype(str).str.startswith(v)]
                else:
                    ret = ret[ret[k] == v]

        if data_group_by is not None:
            ret = ret.groupby(data_group_by.data_fields)
            if data_fields is not None: ret = ret[data_fields]
            if data_group_by.operation == MathOperation.sum:
                ret = ret.sum()
            elif data_group_by.operation == MathOperation.count:
                ret = ret.count()
            elif data_group_by.operation == MathOperation.min:
                ret = ret.min()
            elif data_group_by.operation == MathOperation.max:
                ret = ret.max()
            elif data_group_by.operation == MathOperation.mean:
                ret = ret.mean()
            if data_group_by.unstack:
                ret = ret.unstack()
                ret.columns = ret.columns.droplevel()
        elif data_fields is not None:
            ret = ret[data_fields]

        if data_sort:
            ret = ret.sort_values(by=data_sort.field, ascending=data_sort.type != DataSortType.ASC)

        if data_parse is not None:
            for k, v in data_parse.items():
                ret[k] = v(ret[k])

        total_count = len(ret)
        ret = ret.iloc[offset:(len(ret) if limit < 0 else min(offset + limit, len(ret)))]
        return GetAllResponse(list=self._calculate_schema(ret, convert2schema), count=total_count)

    def _save(self):
        if self.file_path is not None:
            self.df.to_csv(self.file_path)

    def create(self, entry):
        if entry.dict()[self.column_id] in self.df.index:
            raise HTTPException(status.HTTP_409_CONFLICT, detail="Already Exists")
        new = pd.json_normalize(entry.dict()).set_index(self.column_id)
        self.df = self.df.append(new)
        self._save()
        return entry

    def get_or_create(self, entry, data_filter: Dict = None):
        entries = self.get_all(data_filter=data_filter).list
        if len(entries) > 0:
            return entries[0]
        return self.create(entry)

    def delete(self, entry_id: int):
        if entry_id not in self.df.index:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Not found")
        self.df = self.df.drop(entry_id)
        self._save()

    def edit(self, entry_id: int, entry, commit=True):
        if entry_id not in self.df.index:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Not found")
        new = pd.json_normalize(entry.dict()).iloc[0].dropna()
        self.df.loc[entry_id, new.index] = new
        self._save()
        return self.get(entry_id)

    def count(self, data_filter: Dict = None):
        ret = self.df

        if data_filter:
            for k, v in data_filter.items():
                ret = ret[ret[k] == v]

        total_count = len(ret)
        return total_count
