#%%
import mysql.connector
import pandas as pd
from pymongo import MongoClient
import time
import csv
from statistics import mean
import mysql.connector


#%%
# -----------------------------
# CONNECT TO DATABASES  
# -----------------------------

# Connect to MySQL database
def connect_mysql():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="Hersi_001@112",
        database="book_db"
        )

#Connect to MongoDB database
def connect_mongodb():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["book_db"]
    return db["books"]



#%%time
# -----------------------------
# QUERY DEFINITIONS
# -----------------------------

# Query to filter books with star rating greater than 4
def mysql_simple_filter(cursor):
    cursor.execute("SELECT * FROM books WHERE star_rating > 4")
    return cursor.fetchall()

def mongo_simple_filter(collection):
    return list(collection.find({"star_rating": {"$gt": 4}}))

# Query to calculate average price of books
def mysql_aggregation(cursor):
    cursor.execute("SELECT AVG(price) FROM books")
    return cursor.fetchall()

def mongo_aggregation(collection):
    return list(collection.aggregate([
        {"$group": {"_id": None, "avgPrice": {"$avg": "$price"}}}
    ]))

#  Query to sort books by price in descending order
def mysql_sorting(cursor):
    cursor.execute("SELECT * FROM books ORDER BY price DESC")
    return cursor.fetchall()

def mongo_sorting(collection):
    return list(collection.find().sort("price", -1))


#%%time
# -----------------------------
# INDEX MANAGEMENT
# -----------------------------

def drop_mysql_indexes(cursor):
    try:
        cursor.execute("DROP INDEX idx_unique_title ON books")
    except:
        pass
    try:
        cursor.execute("DROP INDEX idx_composite ON books")
    except:
        pass

def create_mysql_indexes(cursor):
    cursor.execute("CREATE UNIQUE INDEX idx_unique_title ON books (book_title)")
    cursor.execute("CREATE INDEX idx_composite ON books (price, star_rating)")

def drop_mongo_indexes(collection):
    collection.drop_indexes()

def create_mongo_indexes(collection):
    collection.create_index("book_title", unique=True)
    collection.create_index([("price", 1), ("star_rating", -1)])

# -----------------------------
# TIMING FUNCTION
# -----------------------------

def measure_time(func, obj, runs=5):
    times = []

    for _ in range(runs):
        start = time.time()
        func(obj)
        end = time.time()
        times.append(end - start)

    return mean(times)

# -----------------------------
# BENCHMARK FUNCTION
# -----------------------------

def run_benchmark():
    mysql_conn = connect_mysql()
    mysql_cursor = mysql_conn.cursor()

    mongo_collection = connect_mongodb()

    results = []

    # -------------------------
    # WITHOUT INDEXES
    # -------------------------
    print("Running WITHOUT indexes...")

    drop_mysql_indexes(mysql_cursor)
    drop_mongo_indexes(mongo_collection)

    results.append([
        "Simple Filter (No Index)",
        measure_time(mysql_simple_filter, mysql_cursor),
        measure_time(mongo_simple_filter, mongo_collection)
    ])

    results.append([
        "Aggregation (No Index)",
        measure_time(mysql_aggregation, mysql_cursor),
        measure_time(mongo_aggregation, mongo_collection)
    ])

    results.append([
        "Sorting (No Index)",
        measure_time(mysql_sorting, mysql_cursor),
        measure_time(mongo_sorting, mongo_collection)
    ])

    # -------------------------
    # WITH INDEXES
    # -------------------------
    print("Running WITH indexes...")

    create_mysql_indexes(mysql_cursor)
    create_mongo_indexes(mongo_collection)

    results.append([
        "Simple Filter (With Index)",
        measure_time(mysql_simple_filter, mysql_cursor),
        measure_time(mongo_simple_filter, mongo_collection)
    ])

    results.append([
        "Aggregation (With Index)",
        measure_time(mysql_aggregation, mysql_cursor),
        measure_time(mongo_aggregation, mongo_collection)
    ])

    results.append([
        "Sorting (With Index)",
        measure_time(mysql_sorting, mysql_cursor),
        measure_time(mongo_sorting, mongo_collection)
    ])

    return results

# -----------------------------
# SAVE RESULTS
# -----------------------------

def save_results(results, filename="results.csv"):
    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Query Type", "MySQL Time (s)", "MongoDB Time (s)"])
        writer.writerows(results)

# -----------------------------
# RUN SCRIPT
# -----------------------------

if __name__ == "__main__":
    results = run_benchmark()

    for row in results:
        print(f"{row[0]} → MySQL: {row[1]:.5f}s | MongoDB: {row[2]:.5f}s")

    save_results(results)
