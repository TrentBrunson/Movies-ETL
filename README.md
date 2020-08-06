# Movies-ETL
-------------------------

    # 1. Assuming both sets of title data are good - selecting Kaggle
    # 2. Kaggle data has more accurate running time data; Wiki data filled in with zeros 
    # 3. Budget - wiki data not as consistent as Kaggle
    # 4. Box Office - similar results as budget, stick with same source
    # 5. Release data - all Kaggle data has release dates
    # 6. Language - more info in Wiki but Kaggle is more consistent for time saved in cleaning
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
