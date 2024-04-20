"""
Database Connection Utility

This module provides a utility function to establish a connection to a PostgreSQL database using SQLAlchemy
"""

import sys
import os

file_path = os.getenv('WORK_DIR')

sys.path.append(os.path.abspath(file_path))

from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import os
import logging as log
import json

log.basicConfig(level=log.INFO)

with open(f'{file_path}/db_settings.json', 'r') as file:
    credentials = json.load(file)

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
    dialect = credentials.get('PGDIALECT')
    user = credentials.get('PGUSER')
    passwd = credentials.get('PGPASSWD')
    host = credentials.get('PGHOST')
    port = credentials.get('PGPORT')
    db = credentials.get('PGDB')

    url = f"{dialect}://{user}:{passwd}@{host}:{port}/{db}"


    try:
        if not database_exists(url):
            create_database(url)
            log.info(f'Database  created successfully!')
        
        engine = create_engine(url)
        log.info(f'Conected successfully to database!')
        return engine
    except SQLAlchemyError as e:
        log.error(f'Error: {e}')

"""
 Make sure to replace the placeholder credentials with your actual database credentials.
"""
