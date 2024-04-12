import pandas as pd
import re

class Transformations:

    def __init__(self, file):
        self.df = pd.read_csv(file, sep=',', encoding='utf-8')

    def insert_id(self) -> None:
        self.df.insert(0, 'id', self.df.index + 1)
    
    def convert_to_datetime(self, column_name):
        """
        Converts a column in the DataFrame to datetime type.

        Parameters:
            column_name (str): The name of the column to convert.

        Returns:
            pandas.DataFrame: The DataFrame with the column converted to datetime type.
        """
        self.df[column_name] = pd.to_datetime(self.df[column_name], format='%Y').dt.year
        return self.df


################################### Generic Transformations ###################################

def drop_columns(df : pd.DataFrame , columns : list) -> None:
    """
    Drops columns from the DataFrame.

    Parameters:
        columns (list): List of column names to be dropped.
    """
    df.drop(columns=columns, inplace=True)


def drop_null_values(df : pd.DataFrame, columns : list) -> None:
    """
    Drops rows with null values in the DataFrame.
    """
    df.dropna(subset=columns, inplace=True)

def drop_duplicates(df: pd.DataFrame) -> None:
    """
    Drops duplicate rows in the DataFrame.
    """
    df.drop_duplicates(inplace=True)


def fill_null_values(df : pd.DataFrame, columns : list, word : str ) -> None:
    """
    Fill missing values in columns

    Parameters:
        df (DataFrame): The DataFrame containing Grammy nominations information.

    Returns:
        DataFrame: The DataFrame with missing values in 'workers' filled.
    """
    pd.set_option('future.no_silent_downcasting', True)

    df[columns] = df[columns].fillna(word)


################################### Transformations Grammy Dataset ###################################

def clean_and_fill_artist(df: pd.DataFrame) -> None:
    """
    Clean and fill the DataFrame 'df' based on the defined patterns.
    
    Parameters:
        df (DataFrame): The DataFrame containing Grammy nominations information.
        
    Returns:
        DataFrame: The cleaned and filled DataFrame.
    """
    pd.set_option('future.no_silent_downcasting', True)

    patterns = [
        r'songwriters? \((.*?)\)',
        r'([^,]+), soloist',
        r'composer \((.*?)\)',
        r'arrangers? \((.*?)\)',
        r'\((.*?)\)' 
    ]

    extracted_nominees = []

    for pattern in patterns:
        extracted_nominees.append(df['workers'].str.extract(pattern, flags=re.IGNORECASE))

    df['extracted_nominee'] = pd.concat(extracted_nominees, axis=1).ffill(axis=1).iloc[:, -1]

    df['artist'] = df['artist'].fillna(df['extracted_nominee'])

    df.drop(columns=['extracted_nominee'], inplace=True)


def clean_and_fill_workers(df : pd.DataFrame) -> None:
    """
    Fill missing values in 'workers' column with corresponding values from 'artist' column
    where 'artist' has a value and 'workers' is null.

    Parameters:
        df (DataFrame): The DataFrame containing Grammy nominations information.

    Returns:
        DataFrame: The DataFrame with missing values in 'workers' filled.
    """

    mask = df['artist'].notnull() & df['workers'].isnull()

    df.loc[mask, 'workers'] = df.loc[mask, 'artist']

def rename_columns(df: pd.DataFrame, columns: dict) -> None:
    """
    Renames the columns of the DataFrame.

    Parameters:
        columns (dict): Dictionary with columns to be renamed.
    """
    df.rename(columns=columns, inplace=True)


################################### Transformations Spotify Dataset ###################################

def categorize_column(df : pd.DataFrame, column_name : str, bins : list, labels : list) -> None:
    """
    Categorizes a column in the DataFrame based on specified bins and labels.

    Parameters:
        df (pandas.DataFrame): The DataFrame.
        column_name (str): The name of the column to categorize.
        bins (list): The bin edges for categorization.
        labels (list): The labels for the categories.

    Returns:
        None
    """
    df[column_name + '_category'] = pd.cut(df[column_name], bins=bins, labels=labels, right=False)



def convert_duration(df : pd.DataFrame, new_column_name : str, column_name : str) -> None:
    """
    Converts a duration column in milliseconds to a new column with format MM:SS.

    Parameters:
        df (pandas.DataFrame): The DataFrame.
        new_column_name (str): The name for the new duration column.
        column_name (str): The name of the original duration column.

    Returns:
        None
    """
    df[new_column_name] = pd.to_datetime(df[column_name], unit='ms').dt.strftime('%M:%S')


def map_genre_to_category(df : pd.DataFrame) -> None:
    """
    Maps specific genres to broader categories and adds a new 'genre' column to the DataFrame.

    Parameters:
        df (pandas.DataFrame): The DataFrame containing the 'track_genre' column.

    Returns:
        None
    """
    genre_mapping = {
        'mood': ['ambient', 'chill', 'happy', 'sad', 'sleep', 'study', 'comedy'],
        'electronic': ['afrobeat', 'breakbeat', 'chicago-house', 'club', 'dance', 'deep-house', 'detroit-techno', 'dub', 'dubstep', 'edm', 'electro', 'electronic', 'house', 'idm', 'techno', 'minimal-techno', 'trance', 'hardstyle'],
        'pop': ['anime', 'cantopop', 'j-pop', 'k-pop', 'pop', 'power-pop', 'synth-pop', 'indie-pop', 'pop-film'],
        'urban': ['hip-hop', 'j-dance', 'j-idol', 'r-n-b', 'trip-hop'],
        'latino': ['brazil', 'latin', 'latino', 'reggaeton', 'salsa', 'samba', 'spanish', 'pagode', 'sertanejo', 'mpb'],
        'global sounds': ['indian', 'iranian', 'malay', 'mandopop', 'reggae', 'turkish', 'ska', 'dancehall', 'tango'],
        'jazz and soul': ['blues', 'bluegrass', 'funk', 'gospel', 'jazz', 'soul'],
        'varied themes': ['children', 'disney', 'forro', 'grindcore', 'kids', 'party', 'romance', 'show-tunes'],
        'instrumental': ['acoustic', 'classical', 'folk', 'guitar', 'piano', 'singer-songwriter', 'songwriter', 'world-music', 'opera', 'new-age'],
        'single genre': ['country', 'progressive-house', 'swedish', 'emo', 'honky-tonk', 'french', 'german', 'drum-and-bass', 'groove', 'disco'],
        'rock and metal': ['alt-rock', 'alternative', 'british', 'grunge', 'hard-rock', 'indie', 'metal', 'metalcore', 'punk-rock', 'rock', 'rock-n-roll', 'black-metal', 'death-metal', 'hardcore', 'heavy-metal', 'industrial', 'psych-rock', 'rockabilly', 'goth', 'punk', 'j-rock', 'garage']
    }

    genre_to_category = {genre: category for category, genres in genre_mapping.items() for genre in genres}

    df['genre'] = df['track_genre'].map(genre_to_category)
