import typing as t

from fastapi import HTTPException
from sqlalchemy.orm import Session

from fastapi_crud_orm_connector.api.security import get_password_hash
from fastapi_crud_orm_connector.orm.crud import Crud, GetAllResponse
from fastapi_crud_orm_connector.orm.dict_crud import DictCrud
from fastapi_crud_orm_connector.orm.mongodb_crud import MongoDBCrud
from fastapi_crud_orm_connector.orm.rdb_crud import RDBCrud
from fastapi_crud_orm_connector.schemas import user_schema, SecretUser, pandas_user_schema
from fastapi_crud_orm_connector.utils.pydantic_schema import SchemaBase, PandasSchema
from fastapi_crud_orm_connector.utils.rdb_models import User
from fastapi_crud_orm_connector.utils.rdb_session import Base


def dict_user_crud(data: t.List[t.Dict],
                   schema: PandasSchema = pandas_user_schema):
    return UserCrud(DictCrud(data, schema))


def rdb_user_crud(user_model: Base = User, schema: SchemaBase = user_schema, db: Session = None):
    return UserCrud(RDBCrud(user_model, dict(user=user_model), schema, db))


def mdb_user_crud(schema: SchemaBase = user_schema, db=None):
    return UserCrud(MongoDBCrud('users', schema, db), id_column='_id')


class UserCrud:
    def __init__(self, crud: Crud, id_column='id'):
        self.crud = crud
        self.id_column = id_column

    def get_schema(self):
        return self.crud.schema.instance

    def use_db(self, db):
        self.crud.use_db(db)
        return self

    def get_user(self, user_id):
        try:
            return self.crud.get_first(data_filter={self.id_column: user_id})
        except Exception as e:
            raise HTTPException(status_code=404, detail="User not found")

    def get_user_by_email(self, email: str, include_password: bool = False):
        try:
            schema = True if not include_password else SecretUser
            return self.crud.get_first(data_filter={'email': email}, convert2schema=schema)
        except Exception as e:
            # raise HTTPException(status_code=404, detail="User not found")
            print(f'User not found {email}')
            return None

    def get_users(self, skip: int = 0, limit: int = 100) -> GetAllResponse:
        return self.crud.get_all(offset=skip, limit=limit)

    def create_user(self, user):
        hashed_password = get_password_hash(user.password)
        return self.crud.create(
            SecretUser(
                first_name=user.first_name,
                last_name=user.last_name,
                email=user.email,
                is_active=user.is_active,
                is_superuser=user.is_superuser,
                hashed_password=hashed_password, )
        )

    def delete_user(self, user_id):
        return self.crud.delete(user_id)

    def edit_user(self, user_id, user):
        update_data = user.dict(exclude_unset=True)

        if "password" in update_data:
            update_data["hashed_password"] = get_password_hash(user.password)
            del update_data["password"]

        return self.crud.edit(user_id, update_data)
