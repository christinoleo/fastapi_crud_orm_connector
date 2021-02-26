from sqlalchemy import Boolean, Column, Integer, String, Text, Float

from fastapi_crud_orm_connector.utils.rdb_session import Base


class User(Base):
    __tablename__ = "user"
    __table_args__ = {'extend_existing': True}
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    first_name = Column(String)
    last_name = Column(String)
    hashed_password = Column(String, nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    is_superuser = Column(Boolean, nullable=False, default=False)


model_map = {
    User.__tablename__: User,
}
