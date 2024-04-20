import sys
import os

file_path = os.getenv('WORK_DIR')

sys.path.append(os.path.abspath(file_path))


from transformations.transformations import *
from src.models.database_models import GrammyAwards
from src.models.database_models import SongsData
from src.database.db_connection import get_engine
from sqlalchemy import inspect, Table, MetaData, insert, select
import logging as log
import json

from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

log.basicConfig(level=log.INFO)

def grammy_process() -> json:
    """
    Process the Grammy nominations data.

    Returns:
        JSON: The JSON representation of the DataFrame containing the Grammy nominations data.

    Parameters:
        None
    """

    connection = get_engine()


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

        select_stmt = select([table])

        result_proxy = connection.execute(select_stmt)
        results = result_proxy.fetchall()

        column_names = table.columns.keys()
        df_2 = pd.DataFrame(results, columns=column_names)
        return df_2.to_json(orient='records')

    except Exception as e:
        log.error(f"Error processing data: {e}")

def transform_grammys_data(json_data : json) -> json:
    """
    Transform the Grammy nominations data.
    
    Parameters:
        json_data (JSON): The JSON representation of the DataFrame containing the Grammy nominations data.
        
    Returns:
        json_data (JSON): The JSON representation of the transformed DataFrame.
    """

    #log.info('Data type is: ', type(json_data))
    json_data = json.loads(json_data)
    df = pd.DataFrame(json_data)

    log.info('Starting transformations...')

    colums_drop = ['published_at', 'updated_at', 'img']
    drop_columns(df, colums_drop)

    clean_and_fill_artist(df)
    clean_and_fill_workers(df)

    remove_nulls = ['nominee', 'workers', 'artist']
    drop_null_values(df, remove_nulls)

    rename_columns(df, {'winner': 'nominee_status'})

    log.info('Transformations successfully')
    return df.to_json(orient='records')


def read_spotify_data(file_path: str) -> json:
    """
    Read the Spotify data from the given file path.
    
    Parameters:
        file_path (str): The file path to the Spotify data.
        
    Returns:
        json (JSON): The JSON representation of the DataFrame containing the Spotify data.
    """
    df = pd.read_csv(file_path)

    log.info('Spotify data read successfully!')
    return df.to_json(orient='records')


def transform_spotify_data(json_data : json) -> json:
    """
    Transform the Spotify data.
    
    Parameters:
        json_data (JSON): The JSON representation of the DataFrame containing the Spotify data.      

    Returns:
        json (JSON): The JSON representation of the transformed DataFrame.
    """

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


def merge_datasets(json_data1 : json, json_data2 : json) -> json :
    """
    Merge the two datasets.
    
    Parameters:
        json_data1 (JSON): The first dataset. Grammy Awards   
        json_data2 (JSON): The second dataset. Spotify
        
    Returns:
        df_merged (JSON): The merged dataset.
    """

    json_data1 = json.loads(json_data1)
    dataset1 = pd.DataFrame(json_data1)

    log.info('Data type is: ', type(json_data2))
    json_data2 = json.loads(json_data2)
    dataset2 = pd.DataFrame(json_data2)

    df_merged = dataset2.merge(dataset1, how='left', left_on='track_name', right_on='nominee')


    fill_columns = ['title', 'category']
    fill_null_values(df_merged,fill_columns, 'Not applicable' )

    fill_column = ['nominee_status']
    fill_null_values(df_merged, fill_column, False)

    colums_drop = ['key','mode', 'acousticness','instrumentalness', 'liveness', 'tempo', 'time_signature', 'id', 'year', 'workers', 'artist', 'nominee']
    drop_columns(df_merged, colums_drop)

    log.info('Datasets merged and transformed successfully!')

    log.info('df columns: %s', df_merged.columns)

    return df_merged.to_json(orient='records')

def load_merge(json_data : json) -> json:
    """
    Load the merged dataset to the database.

    Parameters:
        json_data (JSON): The JSON representation of the merged dataset.
    
    Returns:
        json (JSON): The JSON representation of the DataFrame containing the merged dataset.
    """

    json_data = json.loads(json_data)
    df = pd.DataFrame(json_data)

    df.insert(0, 'id', df.index + 1)

    connection = get_engine()

    try:
        if inspect(connection).has_table('songs_data'):
            SongsData.__table__.drop(connection)
            log.info("Table dropped successfully.")
        
        SongsData.__table__.create(connection)
        log.info("Table created successfully.")

        metadata = MetaData()
        table = Table('songs_data', metadata, autoload=True, autoload_with=connection)

        with connection.connect() as conn:
            values = [{col: row[col] for col in df.columns} for _, row in df.iterrows()]

            conn.execute(insert(table), values)

        log.info('Data loaded successfully')

        return df.to_json(orient='records')
    
    except Exception as e:
        log.error(f"Error processing data: {e}")

CREDENCIALS_PATH = 'credentials_module.json'

def login() -> GoogleDrive:
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(CREDENCIALS_PATH)

    if gauth.credentials is None:
        gauth.Refresh()
        gauth.SaveCredentialsFile(CREDENCIALS_PATH)
    else:
        gauth.Authorize()
    return GoogleDrive(gauth)



def load_dataset_to_drive(json_data : json, title : str , folder_id: str) -> None:
    """
    Load the dataset to Google Drive.
    
    Parameters:
        json_data (JSON): The DataFrame to be uploaded.
        title (str): The title of the file.
        folder_id (str): The folder ID to load the dataset to.
        
    Returns:
        None
    """

    json_data = json.loads(json_data)
    df = pd.DataFrame(json_data)

    drive = login()

    csv_string = df.to_csv(index=False) 

    file = drive.CreateFile({'title' : title ,
                             'parents': [{'kind': 'drive#fileLink', 'id': folder_id}],
                             'mimeTypp': 'text/csv'})
    
    file.SetContentString(csv_string) 
    file.Upload()

    log.info('Dataset loaded to Google Drive successfully!')



