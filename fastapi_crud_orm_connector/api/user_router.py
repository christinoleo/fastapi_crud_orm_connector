from typing import List

from fastapi import APIRouter, Request, Depends, Response

from fastapi_crud_orm_connector.api.auth import Authentication
from fastapi_crud_orm_connector.orm.user_crud import UserCrud
from fastapi_crud_orm_connector.schemas import UserCreate, UserEdit


def generate_user_router(r: APIRouter, auth: Authentication, user_crud: UserCrud):
    @r.get("/users",
           response_model=List[user_crud.get_schema()],
           response_model_exclude_none=True, )
    async def users_list(response: Response,
                         db=Depends(auth.get_db),
                         current_user=Depends(auth.get_current_active_superuser)):
        """
        Get all users
        """
        users = user_crud.use_db(db).get_users()
        # This is necessary for react-admin to work
        response.headers["Content-Range"] = f"0-9/{users.count}"
        return users.list

    @r.get("/users/me", response_model=user_crud.get_schema(), response_model_exclude_none=True)
    async def user_me(current_user=Depends(auth.get_current_active_user)):
        """
        Get own user
        """
        return current_user

    @r.get("/users/{user_id}",
           response_model=user_crud.get_schema(),
           response_model_exclude_none=True, )
    async def user_details(request: Request,
                           user_id: str,
                           db=Depends(auth.get_db),
                           current_user=Depends(auth.get_current_active_superuser), ):
        """
        Get any user details
        """
        user = user_crud.use_db(db).get_user(user_id)
        return user
        # return encoders.jsonable_encoder(
        #     user, skip_defaults=True, exclude_none=True,
        # )

    @r.post("/users", response_model=user_crud.get_schema(), response_model_exclude_none=True)
    async def user_create(
            request: Request,
            user: UserCreate,
            db=Depends(auth.get_db),
            current_user=Depends(auth.get_current_active_superuser),):
        """
        Create a new user
        """
        return user_crud.use_db(db).create_user(user)

    @r.put("/users/{user_id}", response_model=user_crud.get_schema(), response_model_exclude_none=True)
    async def user_edit(request: Request,
                        user_id: str,
                        user: UserEdit,
                        db=Depends(auth.get_db),
                        current_user=Depends(auth.get_current_active_superuser), ):
        """
        Update existing user
        """
        return user_crud.use_db(db).edit_user(user_id, user)

    @r.delete("/users/{user_id}", response_model=user_crud.get_schema(), response_model_exclude_none=True)
    async def user_delete(request: Request,
                          user_id: str,
                          db=Depends(auth.get_db),
                          current_user=Depends(auth.get_current_active_superuser), ):
        """
        Delete existing user
        """
        return user_crud.use_db(db).delete_user(user_id)

    return r
