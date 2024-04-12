import sys
import os

# work_dir = os.getenv('WORK_DIR')

# sys.path.append(work_dir)
sys.path.append(os.path.abspath("/opt/airflow/"))


from transformations.transformations import *
from src.models.database_models import GrammyAwards
from src.database.db_connection import get_engine
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import logging as log
import json

from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

log.basicConfig(level=log.INFO)

def connect_to_database():
    """
    Connect to the database.
    
    Parameters:
        None
        
    Returns:
        str: The URL of the database connection.
    """
    url = 'postgresql+psycopg2://airflow:airflow@postgres/airflow'
    connection = create_engine(url)
    
    # Obtener la URL de conexión desde el objeto Engine
    connection_url = str(connection.url)
    
    log.info('Connected to the database successfully!')
    return connection_url

def query_grammy_data() -> pd.DataFrame:
    """
    Query the Grammy data from the database.
    
    Parameters:
        None

    Returns:
        DataFrame: The DataFrame containing the queried data.
    """
    connection = get_engine()
    Session = sessionmaker(bind=connection)
    session = Session()

    try:
        query = session.query(GrammyAwards).statement
        df = pd.read_sql(query, connection)
    except Exception as e:
        log.error(f'Error executing the query: {e}')
        
    log.info('Query executed successfully, df ready to be processed!')
    return df

def transform_grammys_data(df : pd.DataFrame) -> pd.DataFrame:
    """
    Transform the Grammy nominations data.
    
    Parameters:
        df (DataFrame): The DataFrame containing the Grammy nominations data.
        
    Returns:
        DataFrame: The transformed DataFrame.
    """

    colums_drop = ['published_at', 'updated_at', 'img']
    drop_columns(df, colums_drop)

    clean_and_fill_artist(df)
    clean_and_fill_workers(df)

    remove_nulls = ['nominee', 'workers', 'artist']
    drop_null_values(df, remove_nulls)

    rename_columns(df, {'winner': 'nominee_status'})
    
    return df


def read_spotify_data(file_path: str) -> pd.DataFrame:
    """
    Read the Spotify data from the given file path.
    
    Parameters:
        file_path (str): The file path to the Spotify data.
        
    Returns:
        DataFrame: The DataFrame containing the Spotify data.
    """
    df = pd.read_csv(file_path)

    log.info('Spotify data read successfully!')
    return df.to_json(orient='records')


def transform_spotify_data(json_data) :
    """
    Transform the Spotify data.
    
    Parameters:
        df (DataFrame): The DataFrame containing the Spotify data.
        
    Returns:
        DataFrame: The transformed DataFrame.
    """
    log.info('Data type is: ', type(json_data))
    json_data = json.loads(json_data)
    df = pd.DataFrame(json_data)

    columns_drop = ['Unnamed: 0']
    drop_columns(df, columns_drop)

    colums_drop_na = ['artists']
    drop_null_values(df, colums_drop_na)

    drop_duplicates(df)

    categorize_column(df, 'popularity', bins=[0, 25, 50, 75, 101], labels=['Low', 'Medium-Low', 'Medium-High', 'High'])

    convert_duration(df, 'duration_min_sec', 'duration_ms')

    categorize_column(df, 'danceability', bins=[0, 0.25, 0.5, 0.75, 1.0], labels=['Very Low', 'Low', 'Medium', 'High'])

    categorize_column(df, 'speechiness', bins=[0, 0.33, 0.66, 1.0], labels=['Non-speech', 'Music and Speech', 'Speech Only'])

    categorize_column(df, 'valence', bins=[0, 0.5, 0.75, 1.0], labels=['Negative', 'Neutral', 'Positive'])

    map_genre_to_category(df)

    log.info('Spotify data transformed successfully!')
    return df.to_json(orient='records')


def merge_datasets(dataset1 : pd.DataFrame, dataset2: pd.DataFrame) -> pd.DataFrame:
    """
    Merge the two datasets.
    
    Parameters:
        dataset1 (DataFrame): The first dataset.    
        dataset2 (DataFrame): The second dataset.
        
    Returns:
        DataFrame: The merged dataset.
    """
    df_merged = dataset2.merge(dataset1, how='left', left_on='track_name', right_on='nominee')

    fill_columns = ['title', 'category']
    fill_null_values(df_merged,fill_columns, 'Not applicable' )

    fill_column = ['nominee_status']
    fill_null_values(df_merged, fill_column, False)

    colums_drop = ['key','mode', 'acousticness','instrumentalness', 'liveness', 'tempo', 'time_signature', 'id', 'year', 'workers', 'artist', 'nominee']
    drop_columns(df_merged, colums_drop)

    log.info('Datasets merged and transformed successfully!')

    return df_merged




CREDENCIALS_PATH = 'credentials_module.json'

def login():
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(CREDENCIALS_PATH)

    if gauth.credentials is None:
        gauth.Refresh()
        gauth.SaveCredentialsFile(CREDENCIALS_PATH)
    else:
        gauth.Authorize()
    return GoogleDrive(gauth)



def load_dataset_to_drive(df : pd.DataFrame , title : str , folder_id: str):
    """
    Load the dataset to Google Drive.
    
    Parameters:
        df (DataFrame): The DataFrame to be uploaded.
        title (str): The title of the file.
        folder_id (str): The folder ID to load the dataset to.
        
    Returns:
        None
    """
    drive = login()

    csv_string = df.to_csv(index=False) #pasarle el csv directamente

    file = drive.CreateFile({'title' : title ,
                             'parents': [{'kind': 'drive#fileLink', 'id': folder_id}],
                             'mimeTypp': 'text/csv'})
    
    file.SetContentString(csv_string) #cambiar por set content file
    file.Upload()

    log.info('Dataset loaded to Google Drive successfully!')



if __name__ == '__main__':
    
    df_grammy_data = query_grammy_data() # Ejecuta el query y retorna un DataFrame se guarda en df_grammy_data
    df_transformed = transform_grammys_data(df_grammy_data) # Recibe el Dataframe de la función anterior y lo transforma

    df_spotify_data = read_spotify_data('data/spotify_dataset.csv') # Retorna un DataFrame con la data de spotify
    df_transformed_spotify = transform_spotify_data(df_spotify_data) # Recibe el DataFrame de la función anterior y lo transforma

    df_merged = merge_datasets(df_transformed, df_transformed_spotify)

    load_dataset_to_drive(df_merged, 'merged_dataset_prueba1.csv', '1ysWMDSUXVJrr0YmCS7fG3NliEJDbgMRl')
