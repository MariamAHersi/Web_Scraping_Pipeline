#%%
import mysql.connector
import pandas as pd
from pymongo import MongoClient

#%%
# Connect to database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="Hersi_001@112",
    database="book_db")
cursor = conn.cursor()


#%%
#Connect to MongoDB database
client = MongoClient("mongodb://localhost:27017/")
db = client["book_db"]
collection = db["books"]

#  Compare query execution time between MongoDB and MySQL.