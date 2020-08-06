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

    kaggle_metadata = pd.read_csv(f'{file_dir}{kaggle_metafile}', low_memory=False)

    ratings = pd.read_csv(f'{file_dir}{ratings_file}', low_memory=False)  
    
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

# rerunning list comprehension to clean wiki_movies and recreate the DF
clean_movies = [clean_movie(movie) for movie in wiki_movie_file]
wiki_movies_df = pd.DataFrame(clean_movies)






