from sqlalchemy import Column, Integer, String, DateTime, Boolean, Date, Float
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

class SongsData(BASE):
    __tablename__ = 'songs_data'
    id = Column(Integer, primary_key=True)
    track_id = Column(String, nullable=False)
    artists = Column(String, nullable=False)
    album_name = Column(String, nullable=False)
    track_name = Column(String, nullable=False)
    popularity = Column(Float, nullable=False)
    duration_ms = Column(Integer, nullable=False)
    explicit = Column(Boolean, nullable=False)
    danceability = Column(Float, nullable=False)
    energy = Column(Float, nullable=False)
    loudness = Column(Float, nullable=False)
    speechiness = Column(Float, nullable=False)
    valence = Column(Float, nullable=False)
    track_genre = Column(String, nullable=False)
    popularity_category = Column(String, nullable=False)
    duration_min_sec = Column(String, nullable=False)
    danceability_category = Column(String, nullable=False)
    speechiness_category = Column(String, nullable=False)
    valence_category = Column(String, nullable=False)
    genre = Column(String, nullable=False)
    title = Column(String, nullable=False)
    category = Column(String, nullable=False)
    nominee_status = Column(Boolean, nullable=False)
                