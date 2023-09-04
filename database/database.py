"""Database connection classes"""
import os
from sqlalchemy import create_engine as _create_engine
from sqlalchemy.orm import sessionmaker as _sessionmaker


engine = _create_engine(
    f"postgresql://postgres:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}/ml_football")
Session = _sessionmaker(bind=engine)