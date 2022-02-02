from typing import Dict, List, Union, Type, Optional

import pandas as pd
from fastapi import HTTPException
from pydantic import BaseModel
from starlette import status

from fastapi_crud_orm_connector.orm.crud import Crud, GetAllResponse, DataSort, DataSortType, DataGroupBy, MathOperation, DataSimplify
from fastapi_crud_orm_connector.orm.crud_exceptions import CannotFilterFields, CannotGroupBy, CannotNormalize
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
        if convert2schema is False:
            return ret
        return self._calculate_schema(ret, convert2schema)[0]

    def get_all(self, offset: int = 0,
                limit: int = 25,
                data_filter: Dict = None,
                data_sort: DataSort = None,
                data_fields: List = None,
                data_group_by: DataGroupBy = None,
                data_parse: Dict = None,
                data_simplify: List[DataSimplify] = None,
                minimum_rows_allowed: int = 30,
                *,
                weight_column: Optional[str] = None,
                convert2schema: Union[bool, Type[BaseModel]] = True
                ) -> GetAllResponse:

        ret = self.df
        ret = ret.reset_index()
        if self.column_id is not None:
            ret['id'] = ret[self.column_id]

        if weight_column:
            if data_fields is not None:
                ret[data_fields] = ret[data_fields].mul(ret[weight_column], axis=0)
            # if data_group_by is not None:
            #     ret[data_group_by.data_fields] = ret[data_group_by.data_fields].mul(ret[normalization_column], axis=0)
            else:
                raise CannotNormalize('Need to specify a data filter for normalization')

        if data_filter is not None:
            for k, v in data_filter.items():
                if isinstance(v, list):
                    ret = ret[ret[k].isin(v)]
                elif isinstance(v, str):
                    ret = ret[ret[k].astype(str).str.startswith(v)]
                else:
                    ret = ret[ret[k] == v]

        _filter_by_index = None
        if minimum_rows_allowed and data_group_by:
            _filter_by_index = ret[data_group_by.data_fields + data_fields].dropna()[data_group_by.data_fields[0]].value_counts()

        if data_group_by is not None:
            if not set(self.df.columns).issuperset(set(data_group_by.data_fields)):
                raise CannotGroupBy(data_group_by.data_fields)

            ret = ret.groupby(data_group_by.data_fields)
            if data_fields is not None:
                if not set(self.df.columns).issuperset(set(data_fields)):
                    raise CannotFilterFields(data_fields)
                ret = ret[data_fields]
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
            if not set(self.df.columns).issuperset(set(data_fields)):
                raise CannotFilterFields(data_fields)
            ret = ret[data_fields]

        if data_simplify:
            _ret = ret.reset_index()
            for s in data_simplify:
                toreplace = _ret[_ret[s.data_field].isin(s.data_from)]
                _new_line = None
                if data_group_by.operation == MathOperation.sum:
                    _new_line = toreplace.sum()
                elif data_group_by.operation == MathOperation.count:
                    _new_line = toreplace.sum()
                elif data_group_by.operation == MathOperation.min:
                    _new_line = toreplace.min()
                elif data_group_by.operation == MathOperation.max:
                    _new_line = toreplace.max()
                elif data_group_by.operation == MathOperation.mean:
                    _new_line = toreplace.mean()
                _new_line[s.data_field] = s.data_to
                ret = ret.drop(toreplace[s.data_field])
                if not minimum_rows_allowed or _filter_by_index[toreplace[s.data_field]].sum() >= minimum_rows_allowed:
                    ret = ret.reset_index().append(_new_line, ignore_index=True).set_index(s.data_field)

        if _filter_by_index is not None:
            ret = ret[~ret.index.isin(_filter_by_index.index[_filter_by_index<minimum_rows_allowed])]

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
