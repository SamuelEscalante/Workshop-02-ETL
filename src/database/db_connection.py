"""
Database Connection Utility

This module provides a utility function to establish a connection to a PostgreSQL database using SQLAlchemy
"""
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import os
import logging as log

log.basicConfig(level=log.INFO)

def get_engine():
    """
    Establish a connection to a PostgreSQL database using SQLAlchemy

    Parameters
    ----------
    credentials : dict
        A dictionary containing the database connection credentials

    Returns
    -------
    engine : sqlalchemy.engine.base.Engine
        A SQLAlchemy Engine object representing the established connection.

    Raises
    ------
    SQLAlchemyError: If there is an error establishing the database connection.

    """
    dialect = os.getenv('PGDIALECT')
    user = os.getenv('PGUSER')
    passwd = os.getenv('PGPASSWD')
    host = os.getenv('PGHOST')
    port = os.getenv('PGPORT')
    db = os.getenv('PGDB')

    url = f"{dialect}://{user}:{passwd}@{host}:{port}/{db}"
    try:
        if not database_exists(url):
            create_database(url)
            log.info(f'Database {db} created successfully!')
        
        engine = create_engine(url)
        log.info(f'Conected successfully to database {db}!')
        return engine
    except SQLAlchemyError as e:
        log.error(f'Error: {e}')

"""
 Make sure to replace the placeholder credentials with your actual database credentials.
"""
