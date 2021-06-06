from operator import and_
from typing import Dict, List, Type, Optional, Union

from fastapi import HTTPException, status
from pydantic.main import BaseModel
from sqlalchemy import String as ormString
from sqlalchemy import func
from sqlalchemy.orm import Session

from fastapi_crud_orm_connector.orm.crud import Crud, DataSortType, DataSort, GetAllResponse
from fastapi_crud_orm_connector.utils.pydantic_schema import SchemaBase, orm2pydantic
from fastapi_crud_orm_connector.utils.rdb_session import Base


class RDBCrud(Crud):
    def __init__(self, model: Base, model_map: Dict[str, Base], schema: SchemaBase = None, db: Session = None):
        super().__init__(schema if schema is not None else orm2pydantic(model))
        self.db = db
        self.model = model
        self.model_map = model_map

    def get(self, entry_id: int, convert2schema: Optional[Union[bool, Type[BaseModel]]] = True):
        ret = self.db.query(self.model).filter(self.model.id == entry_id).first()
        if not ret:
            raise HTTPException(status_code=404, detail="not found")
        custom_converter = convert2schema
        if convert2schema is True:
            custom_converter = self.schema.instance.from_orm
        return self._calculate_schema(ret, custom_converter)

    @staticmethod
    def _generate_filter(model, field, value):
        _inner = getattr(model, field)
        if hasattr(_inner, 'type'):
            if isinstance(_inner.type, ormString):  # text
                ret = _inner.ilike(f'%{value}%')
            elif isinstance(value, list):
                ret = _inner.in_(value)
            else:  # number
                ret = _inner == value
            return ret

    def _generate_filters(self, data_filter, query):
        if data_filter is not None and len(data_filter.keys()) > 0:
            _filter = []
            for field, value in data_filter.items():
                if isinstance(value, dict):  # relationship filter
                    sub_model = self.model_map[field]
                    query = query.join(sub_model)
                    for sub_field, sub_value in value.items():
                        _filter.append(self._generate_filter(sub_model, sub_field, sub_value))
                else:
                    _filter.append(self._generate_filter(self.model, field, value))
            query = query.filter(and_(*_filter) if len(_filter) > 1 else _filter[0])
        return query

    def _generate_order_by(self, data_sort, query):
        if data_sort is not None:
            if hasattr(self.model, data_sort.field):
                _inner = getattr(self.model, data_sort.field)
            elif data_sort.field.count('.') == 1:
                field_array = data_sort.field.split('.')
                sub_model, sub_field = self.model_map[field_array[0]], field_array[1]
                query = query.join(sub_model)
                _inner = getattr(sub_model, sub_field)
            else:
                return query
            if hasattr(_inner, 'type'):
                if isinstance(_inner.type, ormString):
                    _inner = func.lower(_inner)  # lowercase sorting of str
                if data_sort.type == DataSortType.ASC:
                    query = query.order_by(_inner.asc())
                else:
                    query = query.order_by(_inner.desc())
        return query

    def get_first(self, data_filter: Dict = None, data_fields: List = None, convert2schema: Union[bool, Type[BaseModel]] = True):
        ret = self.db.query(self.model)
        ret = self._generate_filters(data_filter, ret)

        if data_fields is not None:
            ret_list = []
            for e in ret:
                ret_list.append({f: e[i] for i, f in enumerate(data_fields)})
            ret = ret_list

        ret = ret.first()

        if not convert2schema:
            return ret
        else:
            _schema = self.schema.instance if convert2schema is None else convert2schema
            if data_fields is None:
                ret = _schema.from_orm(ret)
            else:
                ret = _schema(**ret)

        return ret

    def get_all(self, offset: int = 0,
                limit: int = 25,
                data_filter: Dict = None,
                data_sort: DataSort = None,
                data_fields: List = None,
                *,
                convert2schema: Optional[Union[bool, Type[BaseModel]]] = True #TODO
                ) -> GetAllResponse:
        ret = self.db.query(self.model)
        if data_fields is not None:
            q = [getattr(self.model, f) for f in data_fields]
            ret = self.db.query(*q)

        # filter
        ret = self._generate_filters(data_filter, ret)

        total_count = ret.count()

        # sorting
        ret = self._generate_order_by(data_sort, ret)

        ret = ret.limit(limit).offset(offset).all()
        if data_fields is not None:
            ret_list = []
            for e in ret:
                ret_list.append({f: e[i] for i, f in enumerate(data_fields)})
            ret = ret_list

        # Convert to schema
        custom_converter = convert2schema
        if convert2schema is True and data_fields is None:
            custom_converter = self.schema.instance.from_orm

        return GetAllResponse(list=self._calculate_schema(ret, custom_converter), count=total_count)

    def create(self, entry):
        db_entry = self.model(**entry.dict())
        self.db.add(db_entry)
        self.db.commit()
        self.db.refresh(db_entry)
        return self.schema.instance.from_orm(db_entry)

    def get_or_create(self, entry, data_filter: Dict = None):
        ret = self.db.query(self.model).filter(data_filter).first()
        if not ret:
            ret = self.create(entry)
        return ret

    def delete(self, entry_id: int):
        entry = self.get(entry_id, False)
        if not entry:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Not found")
        self.db.delete(entry)
        self.db.commit()

    def edit(self, entry_id: int, entry, commit=True):
        db_entry = self.get(entry_id, False)
        if not db_entry:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Not found")
        update_data = entry.dict(exclude_unset=True)

        for key, value in update_data.items():
            setattr(db_entry, key, value)

        if commit:
            self.db.add(db_entry)
            self.db.commit()
            self.db.refresh(db_entry)
        return self.schema.instance.from_orm(db_entry)

    def count(self, data_filter: Dict = None):
        ret = self.db.query(self.model)
        ret = self._generate_filters(data_filter, ret)
        return ret.count()
