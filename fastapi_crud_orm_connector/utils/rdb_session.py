from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import os

SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL")

if SQLALCHEMY_DATABASE_URI is not None:
    engine = create_engine(
        SQLALCHEMY_DATABASE_URI, connect_args={"check_same_thread": False}
    )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    Base = declarative_base()


# Dependency
def get_rdb():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

import os