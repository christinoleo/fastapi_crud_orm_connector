from typing import Dict, List, Union

import pandas as pd
from fastapi import HTTPException

from fastapi_crud_orm_connector.orm.crud import Crud, GetAllResponse, DataSort, DataSortType, DataGroupBy, MathOperation, MetadataTreeRequest
from fastapi_crud_orm_connector.utils.pydantic_schema import SchemaBase, pd2pydantic


class PandasCrud(Crud):
    def __init__(self, df: pd.DataFrame, schema: Union[SchemaBase, str], column_id=None):
        super().__init__(pd2pydantic(schema, df) if isinstance(schema, str) else schema)
        self.df = df
        self.column_id = column_id if column_id else df.columns[0]

    def get(self, entry_id, convert2schema: bool = True):
        ret = self.df.iloc[entry_id]
        if ret is None:
            raise HTTPException(status_code=404, detail="not found")
        return self.schema.instance(**ret.dropna().to_dict())

    def get_all(self, offset: int = 0,
                limit: int = 25,
                data_filter: Dict = None,
                data_sort: DataSort = None,
                data_fields: List = None,
                data_group_by: DataGroupBy = None,
                data_parse: Dict = None,
                to_schema=True):
        ret = self.df

        if data_filter is not None:
            for k, v in data_filter.items():
                if isinstance(v, list):
                    ret = ret[ret[k].isin(v)]
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

        if data_parse:
            for k, v in data_parse.items():
                ret[k] = v(ret[k])

        total_count = len(ret)
        ret = ret.iloc[offset:(len(ret) if limit < 0 else min(offset + limit, len(ret)))]
        if not to_schema:
            return ret
        ret = [self.schema.instance(**v.dropna().to_dict()) for k, v in ret.iterrows()]
        return GetAllResponse(list=ret, count=total_count)

    def generate_tree(self, metadata_request: MetadataTreeRequest):
        tree_list = self.df[:]
        if metadata_request.root:
            tree_list = tree_list[tree_list.path.str.startswith(metadata_request.root)]
        tree_list_path = tree_list.path.str.split('>>', expand=True)
        tree = {e: dict() for e in tree_list_path[0].unique()}
        for i, col in enumerate(tree_list_path.columns[1:]):
            for index, value in tree_list_path[col][tree_list_path[col - 1].notnull()].items():
                ref = tree
                for c in tree_list_path.loc[index, :i].values:
                    ref = ref[c]
                if value is not None:
                    ref[value] = dict()
                    if tree_list_path.loc[index, (col + 1):].notnull().sum() < 1:
                        ref[value]['@id'] = tree_list.loc[index].id
                elif tree_list_path.loc[index, (col):].notnull().sum() < 1:
                    ref['@id'] = tree_list.loc[index].id
        return tree

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
