#%%
# Import the pandas library
import pandas as pd
# Name data frame as raw_data
df_raw = pd.read_csv("../data/raw/raw_data_multithreading.csv", header=0, sep=",")
# Import regex library for cleaning data
import re

print(df_raw.head())
# %%
def clean_books_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    
    # Drop rows missing critical info
    df = df.dropna(subset=['Title', 'UPC'])

    # Remove duplicate books by UPC
    df = df.drop_duplicates(subset=['UPC'])

    # Drop column as it has no useful information for analysis
    df.drop(columns=[ 'Product Type','Number of Reviews'])

    # Remove £ symbol from price and convert to numeric
    df['Price'] = df['Price'].str.replace('Â£', '', regex=False)
    # Convert price to float
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')

    rating_map = {
    'One': 1,
    'Two': 2,
    'Three': 3,
    'Four': 4,
    'Five': 5
    }

    df['Star Rating'] = df['Star Rating'].map(rating_map)
    # Convert star ratings to numeric
    df['Star Rating'] = pd.to_numeric(df['Star Rating'], errors='coerce')
    
    # Extract numbers from the Availability column
    df['Availability'] = df['Availability'].str.extract(r'(\d+)')
    # Convert to numeric
    df['Availability'] = pd.to_numeric(df['Availability'], errors='coerce')
    
    return df

#%%
#Clean the data
df_clean = clean_books_dataframe(df_raw)

#Save cleaned CSV or load to DB
df_clean.to_csv('../data/processed/books_clean.csv', index=False)