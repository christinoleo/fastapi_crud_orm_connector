from typing import List

from fastapi import APIRouter, Request, Depends, Response

from fastapi_crud_orm_connector.api.auth import Authentication
from fastapi_crud_orm_connector.orm.user_crud import UserCrud
from fastapi_crud_orm_connector.schemas import User, UserCreate, UserEdit
from fastapi_crud_orm_connector.utils.session import get_db


def generate_user_router(r: APIRouter, auth: Authentication, user_crud: UserCrud):
    @r.get("/users",
           response_model=List[User],
           response_model_exclude_none=True, )
    async def users_list(response: Response,
                         db=Depends(get_db),
                         current_user=Depends(auth.get_current_active_superuser)):
        """
        Get all users
        """
        users = user_crud.use_db(db).get_users()
        # This is necessary for react-admin to work
        response.headers["Content-Range"] = f"0-9/{len(users)}"
        return users

    @r.get("/users/me", response_model=User, response_model_exclude_none=True)
    async def user_me(current_user=Depends(auth.get_current_active_user)):
        """
        Get own user
        """
        return current_user

    @r.get("/users/{user_id}",
           response_model=User,
           response_model_exclude_none=True, )
    async def user_details(request: Request,
                           user_id: int,
                           db=Depends(get_db),
                           current_user=Depends(auth.get_current_active_superuser), ):
        """
        Get any user details
        """
        user = user_crud.use_db(db).get_user(user_id)
        return user
        # return encoders.jsonable_encoder(
        #     user, skip_defaults=True, exclude_none=True,
        # )

    @r.post("/users", response_model=User, response_model_exclude_none=True)
    async def user_create(
            request: Request,
            user: UserCreate,
            db=Depends(get_db),
            current_user=Depends(auth.get_current_active_superuser),):
        """
        Create a new user
        """
        return user_crud.use_db(db).create_user(user)

    @r.put("/users/{user_id}", response_model=User, response_model_exclude_none=True)
    async def user_edit(request: Request,
                        user_id: int,
                        user: UserEdit,
                        db=Depends(get_db),
                        current_user=Depends(auth.get_current_active_superuser), ):
        """
        Update existing user
        """
        return user_crud.use_db(db).edit_user(user_id, user)

    @r.delete("/users/{user_id}", response_model=User, response_model_exclude_none=True)
    async def user_delete(request: Request,
                          user_id: int,
                          db=Depends(get_db),
                          current_user=Depends(auth.get_current_active_superuser), ):
        """
        Delete existing user
        """
        return user_crud.use_db(db).delete_user(user_id)

    return r
