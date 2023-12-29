"""Database connection classes"""
import os
from sqlalchemy import create_engine as _create_engine
from sqlalchemy.orm import sessionmaker as _sessionmaker


engine = _create_engine(
    f"postgresql://mlfootball_api:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}/mlfootball")
Session = _sessionmaker(bind=engine)
