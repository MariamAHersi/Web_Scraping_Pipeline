#%%

import mysql.connector
import pandas as pd


#%%
# Connect to database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="Hersi_001@112",
    database="book_db")
cursor = conn.cursor()


#%%

# Create 'Books' table
# Max URL length = 2048, we limit max URL size in db = 255
cursor.execute("""
CREATE TABLE IF NOT EXISTS Books (
upc VARCHAR(20) PRIMARY KEY NOT NULL,
book_title VARCHAR(255) NOT NULL,
price DECIMAL(5,2) NOT NULL,
star_rating FLOAT,
availability VARCHAR(50) NOT NULL,
description TEXT,
url VARCHAR(255) NOT NULL
)""")


#%%

# Load CSV
df = pd.read_csv("../data/processed/books_clean.csv")


#%%

# Checking column names
print(df.columns)


#%%

# Inserting data into table
for _, row in df.iterrows():
    sql = """
    INSERT INTO Books (
    upc, book_title, price, star_rating, availability, description, url)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE book_title=VALUES(book_title)"""
    values = (
        row['UPC'],row['Title'],row['Price'], None if pd.isna(row['Star Rating']) else row['Star Rating'],
        None if pd.isna(row['Availability']) else row['Availability'], None if pd.isna(row['Description']) else row['Description'], row['URL']
    )

    try:
        cursor.execute(sql, values)
        print("done")
    except mysql.connector.Error as e:
        print("error")
conn.commit()
cursor.close()
conn.close()
