from sqlalchemy import Column, Integer, String, DateTime, Boolean, Date
from sqlalchemy.orm import declarative_base


BASE = declarative_base()

class GrammyAwards(BASE):
    __tablename__ = 'grammy_awards'
    id  = Column(Integer, primary_key=True)
    year  = Column(Date, nullable=False)
    title  = Column(String, nullable=False)
    published_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    category  = Column(String, nullable=False)
    nominee	 = Column(String, nullable=True)
    artist  = Column(String, nullable=True)
    workers	 = Column(String, nullable=True)
    img = Column(String, nullable=True)
    winner = Column(Boolean, nullable=False)