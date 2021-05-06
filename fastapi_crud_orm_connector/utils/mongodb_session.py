from pymongo import MongoClient
from fastapi_crud_orm_connector.utils.database_session import DatabaseSession


class MongoDBSession(DatabaseSession):
    def __init__(self, url, database_name):
        self.url = url
        self.database_name = database_name
        if url is not None:
            self.engine = MongoClient(url)

    def get_db(self):
        try:
            yield self.engine[self.database_name]
        except NameError as e:
            raise NameError('Mongodb engine not defined', e)
        finally:
            pass
