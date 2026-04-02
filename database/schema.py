#!/usr/bin/env python
# coding: utf-8

# In[ ]:


get_ipython().system('pip install mysql-connector-python')
get_ipython().system('pip install pandas')
import mysql.connector
import pandas as pd


# In[ ]:


# Connect to database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="root",
    database="Packt_db")
cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS Engineering_Books")


# In[ ]:


# Create 'Engineering_Books' table
# Max URL length = 2048, we limit max URL size in db = 255
cursor.execute("""
CREATE TABLE IF NOT EXISTS Engineering_Books (
isbn VARCHAR(20) PRIMARY KEY NOT NULL,
book_title TEXT NOT NULL,
author_names VARCHAR(255) NOT NULL,
publication_date DATE NOT NULL,
star_rating FLOAT,
num_of_ratings INT,
price DECIMAL(5,2) NOT NULL,
book_format VARCHAR(10) NOT NULL,
url VARCHAR(255) NOT NULL
)""")


# In[ ]:


# Load CSV
Packt_df = pd.read_csv("engineering_books_from_Packt.csv")


# In[ ]:


# Checking column names
print(Packt_df.columns)


# In[ ]:


# Convert date to sql format
Packt_df['Publication Date'] = pd.to_datetime(Packt_df['Publication Date']).dt.strftime('%Y-%m-%d')


# In[ ]:


# Inserting data into table
for _, row in Packt_df.iterrows():
    sql = """
    INSERT INTO Engineering_Books (
    isbn, book_title, author_names, publication_date, star_rating, num_of_ratings, price, book_format, url)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE book_title=VALUES(book_title)"""
    values = (
        row['ISBN'],row['Title'],row['Authors'],row['Publication Date'], None if pd.isna(row['Star Rating']) else row['Star Rating'],
        None if pd.isna(row['Number of Ratings']) else row['Number of Ratings'], row['Price'], row['Format'], row['URL']
    )

    try:
        cursor.execute(sql, values)
        print("done")
    except mysql.connector.Error as e:
        print("error")
conn.commit()
cursor.close()
conn.close()

