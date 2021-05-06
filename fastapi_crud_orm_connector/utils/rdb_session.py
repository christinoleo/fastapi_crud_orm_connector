from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from fastapi_crud_orm_connector.utils.database_session import DatabaseSession

Base = declarative_base()


class RDBSession(DatabaseSession):
    def __init__(self, url):
        self.url = url
        if url is not None:
            self.engine = create_engine(url, connect_args={"check_same_thread": False})
            self.session_local = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def get_db(self):
        try:
            db = self.session_local()
            try:
                yield db
            finally:
                db.close()
        except NameError as e:
            raise NameError('RDB engine not defined', e)
