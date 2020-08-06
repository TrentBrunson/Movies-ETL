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

# declare file path
file_dir = '/Users/bbiz2/dataAustin2020/Movies-ETL/'

# declare globals
wiki_movies = 'wikipedia.movies.json'
kaggle_metadata = 'movies_metadata.csv'
ratings = 'ratings.csv'

# ------------------------------------------------------------------
# Extract data from Wiki, Kaggle and ratings

# Create a function that takes in three arguments:
    # Wikipedia data
    # Kaggle metadata
    # MovieLens rating data (from Kaggle)
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
clean_movies = [clean_movie(movie) for movie in wiki_movies]
wiki_movies_df = pd.DataFrame(clean_movies)

load_movies(wiki_movie_file, kaggle_metafile, ratings_file)
# load_movies(wiki_movies, kaggle_metadata, ratings)

x = len(load_movies)

print(x)