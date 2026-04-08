#%%
#Connect to MongoDB database
from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017/")
db = client["book_db"]
collection = db["books"]

#%%
# Query the MongoDB database to filter and retrieve books based on price.

# Query the MongoDB database to filter and retrieve books based on price

# Query the MongoDB database to filter and retrieve books based on price



#%%
# Query to show all rows

# Query to search for specific title

# Query to show rows with a star rating > 3

# Query to show books in ascending order of price 

# Query to show books with avaialbility > 3 