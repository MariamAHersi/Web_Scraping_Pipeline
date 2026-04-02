#%%
# Import the pandas library
import pandas as pd
# Name data frame as raw_data
df_raw = pd.read_csv("../data/raw/raw_data.csv", header=0, sep=",")

print(df_raw.head())
# %%
def clean_books_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    
    # Drop rows missing critical info
    df = df.dropna(subset=['Title', 'ISBN'])

    # Convert star ratings to numeric
    df['Star Rating'] = pd.to_numeric(df['Star Rating'], errors='coerce')

    # Convert number of ratings to numeric
    df['Number of Ratings'] = pd.to_numeric(df['Number of Ratings'], errors='coerce')

    # Convert price to float (remove £ symbol)
    df['Price'] = df['Price'].str.replace('£', '', regex=False)
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')

    # Convert publication date to datetime
    df['Publication Date'] = pd.to_datetime(df['Publication Date'], errors='coerce')

    # Remove duplicate books by ISBN
    df = df.drop_duplicates(subset=['ISBN'])

    # Standardise text columns
    text_cols = ['Title', 'Authors', 'Format']
    for col in text_cols:
        df[col] = df[col].str.strip()

    return df

#%%
#Clean the data
df_clean = clean_books_dataframe(df_raw)

#Save cleaned CSV or load to DB
df_clean.to_csv('data/processed/books_clean.csv', index=False)