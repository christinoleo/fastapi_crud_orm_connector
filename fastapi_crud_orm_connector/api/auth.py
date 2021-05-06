from fastapi import Depends, HTTPException, status
from jose import jwt, JWTError

from fastapi_crud_orm_connector import schemas
from fastapi_crud_orm_connector.api import security
from fastapi_crud_orm_connector.utils.database_session import DatabaseSession


class Authentication:
    def __init__(self, database_session: DatabaseSession, user_crud, secret_key: str, algorithm: str = "HS256"):
        self.secret_key = secret_key
        self.algorithm = algorithm
        self.user_crud = user_crud
        self.database_session = database_session
        self.get_db = self.database_session.get_db

        async def _get_current_user(db=Depends(database_session.get_db), token: str = Depends(security.oauth2_scheme)):
            credentials_exception = HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
            try:
                payload = jwt.decode(
                    token, self.secret_key, algorithms=[self.algorithm]
                )
                email: str = payload.get("sub")
                if email is None:
                    raise credentials_exception
                permissions: str = payload.get("permissions")
                token_data = schemas.TokenData(email=email, permissions=permissions)
            except JWTError:
                raise credentials_exception
            user = user_crud.use_db(db).get_user_by_email(token_data.email)
            if user is None:
                raise credentials_exception
            return user

        async def _get_current_active_user(current_user=Depends(_get_current_user)):
            if not current_user.is_active:
                raise HTTPException(status_code=400, detail="Inactive user")
            return current_user

        async def _get_current_active_superuser(current_user=Depends(_get_current_user), ):
            if not current_user.is_superuser:
                raise HTTPException(
                    status_code=403, detail="The user doesn't have enough privileges"
                )
            return current_user

        self.get_current_user = _get_current_user
        self.get_current_active_user = _get_current_active_user
        self.get_current_active_superuser = _get_current_active_superuser

    def authenticate_user(self, db, email: str, password: str):
        user = self.user_crud.use_db(db).get_user_by_email(email, include_password=True)
        if not user:
            return False
        if not security.verify_password(password, user.hashed_password):
            return False
        return user

    def sign_up_new_user(self, db, email: str, password: str):
        user = self.user_crud.use_db(db).get_user_by_email(email)
        if user:
            return False  # User already exists
        new_user = self.user_crud.use_db(db).create_user(
            schemas.UserCreate(
                email=email,
                password=password,
                is_active=True,
                is_superuser=False,
            ),
        )
        return new_user
