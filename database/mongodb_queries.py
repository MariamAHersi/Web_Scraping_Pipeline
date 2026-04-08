#%%
#Connect to MongoDB database
from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017/")
db = client["book_db"]
collection = db["books"]

#%%time
# Query to filter and retrieve books based on price.
def filter_books_by_price(max_price):
    query = {"price": {"$lte": max_price}}
    return list(collection.find(query))     
filtered_books = filter_books_by_price(20)
print(filtered_books)

#%%time
# Query to filter and retrieve books based on availability.
def filter_books_by_availability(min_availability):
    query = {"availability": {"$gte": min_availability}}
    return list(collection.find(query))
filtered_books_by_availability = filter_books_by_availability(3)
print(filtered_books_by_availability)


#%%time
# Query to filter and retrieve books based on star rating. 
def filter_books_by_star_rating(min_star_rating):
    query = {"star_rating": {"$gte": min_star_rating}}
    return list(collection.find(query)) 
filtered_books_by_star_rating = filter_books_by_star_rating(4)
print(filtered_books_by_star_rating)    

#%%
# Query to show all rows
def show_all_books():
    return list(collection.find())
all_books = show_all_books()
print(all_books)    

#%%time
# Query to search for specific title
def search_for_title(title):
    query = {"book_title": {"$regex": title, "$options": "i"}}  # Case-insensitive search
    return list(collection.find(query))
searched_books = search_for_title("Python")
print(searched_books)

#%%time
# Query to show rows with a star rating > 3
def show_high_star_rating(min_star_rating):
    query = {"star_rating": {"$gt": min_star_rating}}
    return list(collection.find(query))
high_star_books = show_high_star_rating(3)
print(high_star_books)

#%%time
# Query to show books in ascending order of price 
def show_books_ascending_price():
    return list(collection.find().sort("price", 1))
books_ascending_price = show_books_ascending_price()
print(books_ascending_price)

#%%time
# Query to show books with avaialbility > 3 
def show_books_high_availability(min_availability):
    query = {"availability": {"$gt": min_availability}}
    return list(collection.find(query))
high_availability_books = show_books_high_availability(3)
print(high_availability_books)

#%%time
# Unique Index - to prevent duplicate titles
def create_unique_index_on_title():
    collection.create_index("book_title", unique=True)
unique_title = create_unique_index_on_title()
print(unique_title)

#%%time
# Sparse Index - to only find books with a 5 star rating 
def create_sparse_index_on_star_rating():
    collection.create_index("star_rating", sparse=True)
sparse_star_rating = create_sparse_index_on_star_rating()
print(sparse_star_rating)

#%%time 
# Compound Index - combining columns price and star_rating
def create_compound_index_on_price_and_star_rating():
    collection.create_index([("price", 1), ("star_rating", -1)])   
compound_index = create_compound_index_on_price_and_star_rating()
print(compound_index)