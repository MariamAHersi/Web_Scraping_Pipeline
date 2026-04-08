#%%
from turtle import title
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

#%%time
# 1. Query to extract only three columns: book_title, price, star_rating
def get_filtered_books():
    cursor.execute("SELECT book_title, price, star_rating FROM Books")
    return cursor.fetchall()
columns = get_filtered_books ()
print(columns)


#%%time
# 2. Query to sort the table based on any column: star_rating, price, or book_title 
def get_sorted_books(column_name, ascending=True):
    order = "ASC" if ascending else "DESC"
    query = f"SELECT * FROM Books ORDER BY {column_name} {order}"
    cursor.execute(query)
    return cursor.fetchall()
sorted_books = get_sorted_books("star_rating", ascending=False) 
print(sorted_books)

#%%time
# 3. Query to sort the table based on 5 star rating books
def get_five_star_books():
    query = f"SELECT * FROM Books WHERE star_rating = 5"
    cursor.execute(query)
    return cursor.fetchall()
top_rated_books = get_five_star_books()
print(top_rated_books)

