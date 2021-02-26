import os

from pymongo import MongoClient
from pymongo.client_session import ClientSession

DATABASE_URI = os.getenv("MONGODB_URL")
DATABASE_NAME = os.getenv("MONGODB_DATABASE")

engine = MongoClient(DATABASE_URI)


# Dependency
def get_mdb():
    try:
        yield engine[DATABASE_NAME]
    finally:
        pass
