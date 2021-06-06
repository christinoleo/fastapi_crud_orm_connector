from typing import Dict, List, Type

from fastapi import HTTPException
from pydantic.main import BaseModel
from tinydb import TinyDB, Query

from fastapi_crud_orm_connector.orm.crud import Crud, GetAllResponse, DataSort, DataSortType
from fastapi_crud_orm_connector.utils.pydantic_schema import SchemaBase


class TinyDBCrud(Crud):
    def __init__(self, model: str, schema: SchemaBase, db: TinyDB = None):
        super().__init__(schema)
        self.db = db
        self.model = model

    def get(self, entry_id: int, convert2schema: Optional[Union[bool, Type[BaseModel]]] = None):
        ret = self.db.table(self.model).get(doc_id=entry_id)
        if not ret:
            raise HTTPException(status_code=404, detail="not found")
        return self.schema.instance(**ret) if convert2schema else ret

    def get_first(self, data_filter: Dict = None, data_fields: List = None, convert2schema: Type[BaseModel] = None):
        ret = self.db.table(self.model)
        if data_filter:
            ret = ret.all()
        else:
            ret = ret.search(Query().fragment(data_filter))

        if not ret or len(ret) == 0:
            raise HTTPException(status_code=404, detail="not found")

        for i, r in enumerate(ret):
            for k in r.keys():
                if k not in data_fields:
                    del ret[i][k]

        if convert2schema is None:
            return self.schema.instance(**ret[0])
        elif not convert2schema:
            return ret
        else:
            return convert2schema(**ret[0])

    def get_all(self, offset: int = 0,
                limit: int = 25,
                data_filter: Dict = None,
                data_sort: DataSort = None,
                data_fields: List = None):
        ret = self.db.table(self.model)

        if data_filter:
            ret = ret.all()
        else:
            ret = ret.search(Query().fragment(data_filter))

        if not ret or len(ret) == 0:
            raise HTTPException(status_code=404, detail="not found")

        for i, r in enumerate(ret):
            for k in r.keys():
                if k not in data_fields:
                    del ret[i][k]

        if data_sort is not None:
            ret = sorted(ret, key=lambda i: i[data_sort.field], reverse=data_sort.type == DataSortType.DESC)

        ret = [self.schema.instance(**r) for r in ret]

        total_count = self.count(data_filter)
        ret = ret[offset:offset+limit]

        return GetAllResponse(list=ret, count=total_count)

    def create(self, entry):
        self.db.table(self.model).insert(entry.dict())
        return entry

    def get_or_create(self, entry, data_filter: Dict = None):
        ret = self.db.table(self.model)
        if data_filter is None:
            self.create(entry)
        else:
            self.db.table(self.model).upsert(entry.dict(), cond=Query().fragment(data_filter))

        if not ret or len(ret) == 0:
            return self.create(entry)
        return ret

    def delete(self, entry_id: int):
        self.db.table(self.model).remove(doc_ids=[entry_id])

    def edit(self, entry_id: int, entry, commit=True):
        self.db.table(self.model).update(entry.dict(), doc_ids=[entry_id])

    def count(self, data_filter: Dict = None):
        if data_filter is None:
            return len(self.db.table(self.model))
        return self.db.table(self.model).count(Query().fragment(data_filter))
