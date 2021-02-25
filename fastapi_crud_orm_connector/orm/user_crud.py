import typing as t

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from fastapi_crud_orm_connector.api.security import get_password_hash
from fastapi_crud_orm_connector.orm.crud import Crud
from fastapi_crud_orm_connector.schemas import user_schema
from fastapi_crud_orm_connector.utils.models import User
from fastapi_crud_orm_connector.utils.pydantic_schema import SchemaBase
from fastapi_crud_orm_connector.utils.session import Base


class UserCrud(Crud):
    def __init__(self, user_model: Base = User, schema: SchemaBase = user_schema, db: Session = None):
        super().__init__(schema)
        self.db = db
        self.user_model = user_model

    def get_user(self, user_id: int):
        user = self.db.query(self.user_model).filter(self.user_model.id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return user

    def get_user_by_email(self, email: str):
        return self.db.query(self.user_model).filter(self.user_model.email == email).first()

    def get_users(self, skip: int = 0, limit: int = 100) -> t.List:
        return self.db.query(self.user_model).offset(skip).limit(limit).all()

    def create_user(self, user):
        hashed_password = get_password_hash(user.password)
        db_user = self.user_model(
            first_name=user.first_name,
            last_name=user.last_name,
            email=user.email,
            is_active=user.is_active,
            is_superuser=user.is_superuser,
            hashed_password=hashed_password,
        )
        self.db.add(db_user)
        self.db.commit()
        self.db.refresh(db_user)
        return db_user

    def delete_user(self, user_id: int):
        user = self.get_user(user_id)
        if not user:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="User not found")
        self.db.delete(user)
        self.db.commit()
        return user

    def edit_user(self, user_id: int, user):
        db_user = self.get_user(user_id)
        if not db_user:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="User not found")
        update_data = user.dict(exclude_unset=True)

        if "password" in update_data:
            update_data["hashed_password"] = get_password_hash(user.password)
            del update_data["password"]

        for key, value in update_data.items():
            setattr(db_user, key, value)

        self.db.add(db_user)
        self.db.commit()
        self.db.refresh(db_user)
        return db_user
