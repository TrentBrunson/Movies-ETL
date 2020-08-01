# |---*** Automate ETL pipeline ***---|

# ------------------------------------------------------------------
# Extract data from Wiki, Kaggle and ratings
    # Create a function that takes in three arguments:
        # Wikipedia data
        # Kaggle metadata
        # MovieLens rating data (from Kaggle)


# ------------------------------------------------------------------
# ***
# ------------------------------------------------------------------
# Clean & Transfrom the data to standard formats
    # Set up f(x) to hold code from cleaning work

# ------------------------------------------------------------------
# ***
# ------------------------------------------------------------------
# Load new data into PostgreSQL DB
    # 

# ------------------------------------------------------------------

# Check that the function works correctly on the current Wikipedia and Kaggle data.

# Document any assumptions made

    # 1. Assuming both sets of title data are good - selecting Kaggle
    # 2. Kaggle data has more accurate running time data; Wiki data filled in with zeros 
    # 3. Budget
    # 4. Box Office
    # 5. Release data
    # 6. Langurage
    # 7. Production company
    
    # Resulting assumption table looks like:
        # Wiki                     Movielens                Resolution
        #--------------------------------------------------------------------------
        # title_wiki               title_kaggle             Drop Wiki
        # running_time             runtime                  Keep Kaggle; fill in zeros with Wikipedia data
        # budget_wiki              budget_kaggle
        # box_office               revenue
        # release_date_wiki        release_date_kaggle
        # Language                 original_language
        # Production company(s)    production_companies  

# Use try-except blocks to account for unforeseen problems that may arise with new data

t = 0