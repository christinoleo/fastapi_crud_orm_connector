from typing import Dict, List, Type

from bson import ObjectId
from fastapi import HTTPException
from pydantic.main import BaseModel

from fastapi_crud_orm_connector.orm.crud import Crud, GetAllResponse, DataSort, DataSortType
from fastapi_crud_orm_connector.utils.pydantic_schema import SchemaBase


class MongoDBCrud(Crud):
    def __init__(self, model: str, schema: SchemaBase, db=None):
        super().__init__(schema)
        self.db = db
        self.model = model

    @staticmethod
    def _process_filter(f: Dict):
        if f is None: return dict()
        return {k: ObjectId(v) if k == '_id' else v for k, v in f.items()}

    def get(self, entry_id: str, convert2schema: bool = True):
        ret = self.db[self.model].find_one({'_id': ObjectId(entry_id)})
        if not ret:
            raise HTTPException(status_code=404, detail="not found")
        return self.schema.instance(**ret) if convert2schema else ret

    def get_first(self, data_filter: Dict = None, data_fields: List = None, convert2schema: Type[BaseModel] = None):
        _fields = {f: True for f in data_fields} if data_fields is not None else None
        ret = self.db[self.model].find_one(self._process_filter(data_filter), _fields)
        if not ret:
            raise HTTPException(status_code=404, detail="not found")
        ret['id'] = str(ret['_id'])
        if convert2schema is None: return self.schema.instance(**ret)
        elif not convert2schema: return ret
        else: return convert2schema(**ret)

    def get_all(self, offset: int = 0,
                limit: int = 25,
                data_filter: Dict = None,
                data_sort: DataSort = None,
                data_fields: List = None):
        _fields = {f: True for f in data_fields} if data_fields is not None else None
        ret = self.db[self.model].find(self._process_filter(data_filter), _fields)

        if data_sort is not None:
            _sort = -1 if data_sort.type == DataSortType.DESC else 1
            ret = ret.sort(data_sort.field, _sort)

        total_count = ret.count()
        ret = list(ret.skip(offset).limit(limit))

        # Convert to schema
        for r in ret:
            r['id'] = str(r['_id'])
        ret = [self.schema.instance(**r) for r in ret]

        return GetAllResponse(list=ret, count=total_count)

    def create(self, entry):
        ret = self.db[self.model].insert_one(**entry.dict())
        ret['id'] = str(ret['_id'])
        return self.schema.instance(**ret)

    def get_or_create(self, entry, data_filter: Dict = None):
        ret = self.db[self.model].find_one(self._process_filter(data_filter))
        if not ret:
            return self.create(entry)
        return ret

    def delete(self, entry_id: int):
        self.db[self.model].delete_one({'_id': ObjectId(entry_id)})

    def edit(self, entry_id: int, entry, commit=True):
        self.db[self.model].update_one({'_id': ObjectId(entry_id)}, {'$set': entry})

    def count(self, data_filter: Dict = None):
        return self.db[self.model].find(self._process_filter(data_filter)).count()
