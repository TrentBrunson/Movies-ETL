# |---*** Automate ETL pipeline ***---|

# Document any assumptions made

    # 1. Assuming both sets of title data are good - selecting Kaggle
    # 2. Kaggle data has more accurate running time data; Wiki data filled in with zeros 
    # 3. Budget - wiki data not as consistent as Kaggle
    # 4. Box Office - similar results as budget, stick with same source
    # 5. Release data - all Kaggle data has release dates
    # 6. Langurage - more info in Wiki but Kaggle is more consistent for time saved in cleaning
    # 7. Production company - Kaggle data is more consistent
    
    # Resulting assumption table looks like:
        # Wiki                     Movielens             Resolution
        #--------------------------------------------------------------------------
        # title_wiki               title_kaggle          Drop Wiki
        # running_time             runtime               Keep Kaggle; fill in zeros with Wikipedia data
        # budget_wiki              budget_kaggle         Keep Kaggle; fill in zeros with Wikipedia data
        # box_office               revenue               Keep Kaggle; fill in zeros with Wikipedia data
        # release_date_wiki        release_date_kaggle   Drop Wiki
        # Language                 original_language     Drop Wiki
        # Production company(s)    production_companies  Drop Wiki 

# import dependencies
import os
import pandas as pd
import json
import numpy as np
import re
import time
from sqlalchemy import create_engine
from config import db_password

# declare file path
file_dir = '/Users/bbiz2/dataAustin2020/Movies-ETL/'

# declare globals
wiki_movie_file = 'wikipedia.movies.json'
kaggle_metafile = 'movies_metadata.csv'
ratings_file = 'ratings.csv'

# ------------------------------------------------------------------
# Extract data from Wiki, Kaggle and ratings

# Create a function that takes in three arguments:
    # Wikipedia data
    # Kaggle metadata
    # MovieLens rating data (from Kaggle)
def load_movies(wiki_movies, kaggle_metadata, ratings):
    try: 
        with open(f'{file_dir}/wikipedia.movies.json', mode='r') as file:
            wiki_movies_raw = json.load(file)
    except:
        print(f'Wiki file failed to load.  Check filename and file directory.')

    # creating wiki_movies using list comprehension to remove unnecessary columns
    # filtering out TV shows from movies
    wiki_movies = [movie for movie in wiki_movies_raw 
               if ('Director' in movie or 'Directed by' in movie) 
                   and 'imdb_link' in movie 
               and 'No. of episodes' not in movie]
    try:
        kaggle_metadata = pd.read_csv(f'{file_dir}{kaggle_metafile}', low_memory=False)
    except:
        print('Kaggle file failed to load.  Check filename and file directory.')

    try: 
        ratings = pd.read_csv(f'{file_dir}{ratings_file}', low_memory=False)  
    except:
        print('Ratings file failed to load.  Check filename and file directory.')
    
# ------------------------------------------------------------------


# ------------------------------------------------------------------
# Clean & Transfrom the data to standard formats
    # Set up f(x) to hold code from cleaning work
    # creating function; using list comprehension 

def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy
    alt_titles = {}
    # combine alternate titles into one list
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune–Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)
    if len(alt_titles) > 0:
        movie['alt_titles'] = alt_titles

    # merge column names
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)
    change_column_name('Adaptation by', 'Writer(s)')
    change_column_name('Country of origin', 'Country')
    change_column_name('Directed by', 'Director')
    change_column_name('Distributed by', 'Distributor')
    change_column_name('Edited by', 'Editor(s)')
    change_column_name('Length', 'Running time')
    change_column_name('Original release', 'Release date')
    change_column_name('Music by', 'Composer(s)')
    change_column_name('Produced by', 'Producer(s)')
    change_column_name('Producer', 'Producer(s)')
    change_column_name('Productioncompanies ', 'Production company(s)')
    change_column_name('Productioncompany ', 'Production company(s)')
    change_column_name('Released', 'Release Date')
    change_column_name('Release Date', 'Release date')
    change_column_name('Screen story by', 'Writer(s)')
    change_column_name('Screenplay by', 'Writer(s)')
    change_column_name('Story by', 'Writer(s)')
    change_column_name('Theme music composer', 'Composer(s)')
    change_column_name('Written by', 'Writer(s)')

    return movie

# Calling functions for future use
load_movies(wiki_movie_file, kaggle_metafile, ratings_file)
# rerunning list comprehension to clean wiki_movies and recreate the DF
clean_movies = [clean_movie(movie) for movie in wiki_movie_file]
wiki_movies_df = pd.DataFrame(clean_movies)

# Regular expressions to IMDB IDs for dupes, then drop them
wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)

# list comprehension to look for only columns that are more than 90% filled
wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]

# matching strings in box office
# have two string types "$ddd.d million or billon" & "$ddd,ddd,ddd"
form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'


def parse_dollars(s): # setting function to
    # if s is not a string, return NaN
    if type(s) != str:
        return np.nan

    # if input is of the form $###.# million
    if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
        # remove dollar sign and " million"
        s = re.sub(r'\$|\s|[a-zA-Z]', '', s)
        # convert to float and multiply by a million
        value = float(s) * 10**6
        # return value
        return value

    # if input is of the form $###.# billion
    elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
        # remove dollar sign and " billion"
        s = re.sub(r'\$|\s|[a-zA-Z]','', s)
        # convert to float and multiply by a billion
        value = float(s) * 10**9
        # return value
        return value

    # if input is of the form $###,###,###
    elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
        # remove dollar sign and commas
        s = re.sub(r'\$|,','', s)
        # convert to float
        value = float(s)

        # return value
        return value
    # otherwise, return NaN
    else:
        return np.nan


# clean & merge box_office columns
try: 
    # drop rows in box_office with nulls and put in list
    box_office = wiki_movies_df['Box office'].dropna() 
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    # extract values from box_office and apply the f(x) parse_dollars
    wiki_movies_df['box_office'] = box_office.str.extract(
        f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
except KeyError:
    pass

#  parse release date, making variable to hold non-null values 
# & converting lists to strings
try:
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(
        f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
except KeyError:
    pass

try:
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    wiki_movies_df.drop('Running time', axis=1, inplace=True)
except KeyError:
    pass

kaggle_metafile = pd.read_csv(f'{file_dir}{kaggle_metafile}', low_memory=False)


# Clean Kaggle metadata
kaggle_metafile = kaggle_metafile[kaggle_metafile['adult'] == 'False'].drop('adult',axis='columns') # dropping adult films
kaggle_metafile['video'] = kaggle_metafile['video'] == 'True'  # convert video column to boolean data type
# Setting data types for columns; raising issues if there's mixed data in the column that cannot be converted
kaggle_metafile['budget'] = kaggle_metafile['budget'].astype(int)
kaggle_metafile['id'] = pd.to_numeric(kaggle_metafile['id'], errors='raise')
kaggle_metafile['popularity'] = pd.to_numeric(kaggle_metafile['popularity'], errors='raise')
kaggle_metafile['release_date'] = pd.to_datetime(kaggle_metafile['release_date'])

