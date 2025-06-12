import pymongo
import mysql.connector
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_URI")
client = pymongo.MongoClient(MONGO_URI, tls=True)
mongo_db = client["parking"]  # Renamed to avoid conflict with mysql connection
print("MongoDB collections:", mongo_db.list_collection_names())
collection = mongo_db["availability"]

# Fetch all documents from MongoDB
cursor = collection.find()
data = list(cursor)
print(f"Found {len(data)} documents in MongoDB")

# MySQL setup
mysql_db = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME")
)
mysql_cursor = mysql_db.cursor()

# Prepare the insert query
insert_query = """
INSERT INTO availability (
    timestamp, stationName, parkingAreaName, twoWheelerCapacity,
    threeNFourWheelerCapacity, twoWheelerOccupied, threeNFourWheelerOccupied,
    twoWheelerAvailable, threeNFourWheelerAvailable
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Process and insert data
successful_inserts = 0
failed_inserts = 0
records_to_insert = []

for doc in data:
    try:
        # Extract data from MongoDB document (skip _id field)
        timestamp = doc.get('timestamp')
        station_name = doc.get('stationName')
        parking_area_name = doc.get('parkingAreaName')
        two_wheeler_capacity = doc.get('twoWheelerCapacity')
        three_four_wheeler_capacity = doc.get('threeNFourWheelerCapacity')
        two_wheeler_occupied = doc.get('twoWheelerOccupied')
        three_four_wheeler_occupied = doc.get('threeNFourWheelerOccupied')
        two_wheeler_available = doc.get('twoWheelerAvailable')
        three_four_wheeler_available = doc.get('threeNFourWheelerAvailable')
        
        # Handle timestamp conversion if needed
        if isinstance(timestamp, str):
            # If timestamp is string, parse it
            try:
                timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                # Try alternative format if needed
                print(f"Warning: Could not parse timestamp: {timestamp}")
                timestamp = None
        
        # Prepare record for insertion
        record = (
            timestamp,
            station_name,
            parking_area_name,
            two_wheeler_capacity,
            three_four_wheeler_capacity,
            two_wheeler_occupied,
            three_four_wheeler_occupied,
            two_wheeler_available,
            three_four_wheeler_available
        )
        
        records_to_insert.append(record)
        
    except Exception as e:
        print(f"Error processing document {doc.get('_id', 'unknown')}: {e}")
        failed_inserts += 1

# Bulk insert all records
try:
    if records_to_insert:
        mysql_cursor.executemany(insert_query, records_to_insert)
        mysql_db.commit()
        successful_inserts = len(records_to_insert)
        print(f"Successfully inserted {successful_inserts} records into MySQL")
    else:
        print("No records to insert")
        
except mysql.connector.Error as e:
    print(f"MySQL Error during bulk insert: {e}")
    mysql_db.rollback()
    
    # Try individual inserts if bulk insert fails
    print("Attempting individual inserts...")
    for i, record in enumerate(records_to_insert):
        try:
            mysql_cursor.execute(insert_query, record)
            mysql_db.commit()
            successful_inserts += 1
        except mysql.connector.Error as individual_error:
            print(f"Failed to insert record {i+1}: {individual_error}")
            print(f"Record data: {record}")
            failed_inserts += 1
            mysql_db.rollback()

# Verify the migration
mysql_cursor.execute("SELECT COUNT(*) FROM availability")
mysql_count = mysql_cursor.fetchone()[0]
print(f"\nMigration Summary:")
print(f"MongoDB documents: {len(data)}")
print(f"MySQL records after migration: {mysql_count}")
print(f"Successful inserts: {successful_inserts}")
print(f"Failed inserts: {failed_inserts}")

# Show some sample data from MySQL
print("\nSample data from MySQL:")
mysql_cursor.execute("SELECT * FROM availability LIMIT 5")
sample_records = mysql_cursor.fetchall()
for record in sample_records:
    print(record)

# Clean up connections
mysql_cursor.close()
mysql_db.close()
client.close()

print("\nMigration completed!")