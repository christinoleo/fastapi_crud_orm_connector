import os

from pymongo import MongoClient
from pymongo.client_session import ClientSession

MONGODB_DATABASE_URI = os.getenv("MONGODB_URL")
MONGODB_DATABASE_NAME = os.getenv("MONGODB_DATABASE")


if MONGODB_DATABASE_URI is not None:
    engine = MongoClient(MONGODB_DATABASE_URI)


# Dependency
def get_mdb():
    try:
        yield engine[MONGODB_DATABASE_NAME]
    finally:
        pass
