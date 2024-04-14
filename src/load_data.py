import sys
import os

sys.path.append(os.path.abspath("/home/samuelescalante/prueba_workshop"))

from sqlalchemy import inspect, Table, MetaData, insert, select
from sqlalchemy.orm import sessionmaker
from transformations.transformations import Transformations
from src.models.database_models import GrammyAwards
from src.database.db_connection import get_engine
import pandas as pd
import logging as log

# work_dir = os.getenv('WORK_DIR')

# sys.path.append(work_dir)
def grammy_process():
    connection = get_engine()
    Session = sessionmaker(bind=connection)
    session = Session()

    try:
        if inspect(connection).has_table('grammy_awards'):
            GrammyAwards.__table__.drop(connection)
            log.info("Table dropped successfully.")
        
        GrammyAwards.__table__.create(connection)
        log.info("Table created successfully.")

        transformations = Transformations('data/the_grammy_awards.csv')
        transformations.insert_id()
        transformations.df['year'] = pd.to_datetime(transformations.df['year'], format='%Y')
        log.info("Data transformed successfully.")

        df = transformations.df

        metadata = MetaData()
        table = Table('grammy_awards', metadata, autoload=True, autoload_with=connection)

        with connection.connect() as conn:
            values = [{col: row[col] for col in df.columns} for _, row in df.iterrows()]

            conn.execute(insert(table), values)
        
        log.info('Data loaded successfully')

        log.info('Starting query')

        # Ejecutar una consulta select para obtener todos los registros de la tabla
        select_stmt = select([table])

        # Obtener los resultados de la consulta
        result_proxy = connection.execute(select_stmt)
        results = result_proxy.fetchall()

        # Crear un DataFrame de Pandas a partir de los resultados
        column_names = table.columns.keys()
        df_2 = pd.DataFrame(results, columns=column_names)
        return df_2

    except Exception as e:
        log.error(f"Error processing data: {e}")


print(grammy_process().shape)
