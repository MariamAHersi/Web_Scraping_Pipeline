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
# Query to extract only three columns: book_title, price, star_rating
def get_filtered_books():
    cursor.execute("SELECT book_title, price, star_rating FROM Books")
    return cursor.fetchall()
columns = get_filtered_books ()
print(columns)


#%%time
# Query to sort the table based on any column: star_rating, price, or book_title 
def get_sorted_books(column_name, ascending=True):
    order = "ASC" if ascending else "DESC"
    query = f"SELECT * FROM Books ORDER BY {column_name} {order}"
    cursor.execute(query)
    return cursor.fetchall()
sorted_books = get_sorted_books("star_rating", ascending=False) 
print(sorted_books)

#%%time
# Query to show all rows
def show_all_rows():
    query = f"SELECT * FROM Books"
    cursor.execute(query)
    return cursor.fetchall()
all_rows = show_all_rows()
print(all_rows)

#%%time
# Query to search for specific title
def search_for_title(title):
    query = f"SELECT * FROM books WHERE book_title LIKE '%{title}%'"
    cursor.execute(query)
    return cursor.fetchall()

#%%time
# Query to show rows with a star rating > 3
def show_high_star_rating(ascending=True):
    order = "ASC" if ascending else "DESC"
    query = f"SELECT *FROM books WHERE star_rating > 3"
    cursor.execute(query)
    return cursor.fetchall()
high_rating = show_high_star_rating(ascending = True)
print(high_rating)

#%%time
# Query to show books in ascending order of price 
def show_the_prices(ascending = True ):
    order = "ASC" if ascending else "DESC"
    query = f"SELECT *FROM books ORDER BY price {order}"
    cursor.execute(query)
    return cursor.fetchall()
price_order = show_the_prices (ascending = True)
print(price_order)

#%%time
# Query to show books with avaialbility < 3 
def show_low_availability(ascending=True):
    order = "ASC" if ascending else "DESC"
    query = f"SELECT *FROM books WHERE availability < 3"
    cursor.execute(query)
    return cursor.fetchall()
low_availability = show_low_availability(ascending = True)
print(low_availability)

#%%time
# Unique Index - to prevent duplicate titles
def create_unique_index(column_name):
    query = f"CREATE UNIQUE INDEX idx_unique_{column_name} ON books ({column_name})"
    cursor.execute(query)
    return cursor.fetchall()
unique_index = create_unique_index("book_title")
print(unique_index)
#%%time
# Sparse Index - to only find books with a 5 star rating 
def create_sparse_index(column_name):
    query = f"CREATE INDEX idx_sparse_{column_name} ON books ({column_name}) WHERE star_rating = 5"
    cursor.execute(query)
    return cursor.fetchall()
sparse_index = create_sparse_index("star_rating")
print(sparse_index)

#%%time 
# Compound Index - combining columns price and star_rating
def two_cloumns(column1, column2):
    query = f"CREATE INDEX idx_composite ON books ({column1}, {column2})"
    cursor.execute(query)
    return cursor.fetchall()
compound_index = two_cloumns("price", "star_rating")
print(compound_index)