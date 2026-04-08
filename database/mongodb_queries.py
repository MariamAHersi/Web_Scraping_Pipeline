#%%
from pymongo import MongoClient

#%%
#Connect to MongoDB database
client = MongoClient("mongodb://localhost:27017/")
db = client["book_db"]
collection = db["books"]

#%%time
# 1. Query to filter and retrieve books based on price.
def filter_books_by_price(max_price):
    query = {"price": {"$lte": max_price}}
    return list(collection.find(query))     
filtered_books = filter_books_by_price(20)
print(filtered_books)

#%%time
# 2. Query to filter and retrieve books based on availability.
def filter_books_by_availability(min_availability):
    query = {"availability": {"$gte": min_availability}}
    return list(collection.find(query))
filtered_books_by_availability = filter_books_by_availability(3)
print(filtered_books_by_availability)


#%%time
# 3. Query to filter and retrieve books based on star rating. 
def filter_books_by_star_rating(min_star_rating):
    query = {"star_rating": {"$gte": min_star_rating}}
    return list(collection.find(query)) 
filtered_books_by_star_rating = filter_books_by_star_rating(4)
print(filtered_books_by_star_rating)    
