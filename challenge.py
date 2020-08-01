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
        
#import dependencies
import os
import pandas as pd
import json
import numpy as np
import re
import time
from sqlalchemy import creat_engine
from config import db_password

# declare file path
file_dir = '/Users/bbiz2/dataAustin2020/Movies-ETL/'

# ------------------------------------------------------------------
# Extract data from Wiki, Kaggle and ratings


# Create a function that takes in three arguments:
    # Wikipedia data
    # Kaggle metadata
    # MovieLens rating data (from Kaggle)
def Movies
    with open(f'{file_dir}/wikipedia.movies.json', mode='r') as file:
        wiki_movies_raw = json.load(file)
    # creating wiki_movies using list comprehension to remove unnecessary columns
    wiki_movies = [movie for movie in wiki_movies_raw 
               if ('Director' in movie or 'Directed by' in movie) 
                   and 'imdb_link' in movie 
               and 'No. of episodes' not in movie]
    kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv', low_memory=False)
    ratings = pd.read_csv(f'{file_dir}ratings.csv')  
    
    # call movie cleaning f(x)
    clean_movie(movie)

# ------------------------------------------------------------------
# ***
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
        s = re.sub('\$|\s|[a-zA-Z]', '', s)
        # convert to float and multiply by a million
        value = float(s) * 10**6
        # return value
        return value

    # if input is of the form $###.# billion
    elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
        # remove dollar sign and " billion"
        s = re.sub('\$|\s|[a-zA-Z]','', s)
        # convert to float and multiply by a billion
        value = float(s) * 10**9
        # return value
        return value

    # if input is of the form $###,###,###
    elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
        # remove dollar sign and commas
        s = re.sub('\$|,','', s)
        # convert to float
        value = float(s)

        # return value
        return value
    # otherwise, return NaN
    else:
        return np.nan
    
# extract values from Box_Office and apply the f(x) parse_dollars
wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', 
                           flags=re.IGNORECASE)[0].apply(parse_dollars)


# clean & merge box_office columns
try: 
    # drop rows in box_office with nulls and put in list
    box_office = wiki_movies_df['Box office'].dropna() 
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    # extract values from box_office and apply the f(x) parse_dollars
    wiki_movies_df['box_office'] = box_office.str.extract(
        f'({form_one}|{form_two})',flags=re.IGNORE[0].apply(parse_dollars)                                
 except KeyError:
    pass

# clean budget column
try:
        asdf
except KeyError:
        pass
        
# clean release date
try:
        asdf
except KeyError:
        pass

# clean running time        
try:
        asdf
except KeyError:
        pass

# clean Kaggle metadata
try:
        asdf
except KeyError:
        pass        
        
# clean ratings data
try:
        asdf
except KeyError:
        pass        
        
# ------------------------------------------------------------------
# ***
# ------------------------------------------------------------------
# Load new data into PostgreSQL DB
    # 
    # create a variable for the number of rows imported
def load_db()    
    rows_imported = 0

    # get the start_time from time.time()
    start_time = time.time()

    for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):

        # print out the range of rows that are being imported
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        data.to_sql(name='ratings', con=engine, if_exists='append')
        # increment the number of rows imported by the size of 'data'
        rows_imported += len(data)

        # print that the rows have finished importing
        # add elapsed time to final print out
        print(f'Done. {time.time() - start_time} total seconds elapsed')
# ------------------------------------------------------------------

# Check that the function works correctly on the current Wikipedia and Kaggle data.

 

# Use try-except blocks to account for unforeseen problems that may arise with new data

t = 0