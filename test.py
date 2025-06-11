import pymongo
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_URI")
client = pymongo.MongoClient(MONGO_URI, tls=True) # Change if needed
db = client["parking"]  # Replace with your DB name

print(db.list_collection_names())  # List collections to verify connection

collection = db["availability"]  # Replace with your collection name
cursor = collection.find()

data = list(cursor)  # Fetch all documents

# sql setup
db = mysql.connector.connect(
    host = os.getenv("DB_HOST"),
    user = os.getenv("DB_USER"),
    password = os.getenv("DB_PASSWORD"),
    database = os.getenv("DB_NAME")
)

cursor = db.cursor()

for doc in data:
    for key in doc:
        print(key, doc[key])
    print()