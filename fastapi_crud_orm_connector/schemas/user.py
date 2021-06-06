import typing as t

from pydantic import BaseModel

from fastapi_crud_orm_connector.utils.pydantic_schema import SchemaBase, PandasSchema


class UserBase(BaseModel):
    email: str
    is_active: bool = True
    is_superuser: bool = False
    first_name: str = None
    last_name: str = None


class UserOut(UserBase):
    pass


class UserCreate(UserBase):
    password: str

    class Config:
        orm_mode = True


class UserEdit(UserBase):
    password: t.Optional[str] = None

    class Config:
        orm_mode = True


class User(UserBase):
    id: str

    class Config:
        orm_mode = True


class SecretUser(UserBase):
    hashed_password: str

    class Config:
        orm_mode = True


user_schema = SchemaBase(
    base=UserBase,
    create=UserCreate,
    edit=UserEdit,
    instance=User)

pandas_user_schema = PandasSchema(
    base=UserBase,
    create=UserCreate,
    edit=UserEdit,
    instance=User)


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    email: str = None
    permissions: str = "user"
