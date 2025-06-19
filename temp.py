import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

try:
    mysql_db = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        port=os.getenv("DB_PORT")
    )
    cursor = mysql_db.cursor()
    print("MySQL connection established successfully.")

except mysql.connector.Error as e:
    print(f"MySQL connection error: {e}")
    exit(1)

def get_availability(station_name, vehicle_type):
    """Retrieve today's availability for a station and vehicle type"""
    global cursor
    try:
        query = f"""
        SELECT timestamp, `{vehicle_type}`
        FROM availability
        WHERE stationName = %s
          AND DATE(timestamp) = CURDATE()
        ORDER BY timestamp DESC
        """
        
        cursor.execute(query, (station_name,))
        result = cursor.fetchall()
        
        if result:
            res = []
            for x in result:
                res.append({
                    "timestamp": x[0],
                    "available": x[1]
                })
            return res
        else:
            return None
            
    except mysql.connector.Error as e:
        print(f"MySQL Error in get_availability: {e}")
        return None
    
fetch = get_availability("Guindy", "twoWheelerAvailable")

for x in fetch:
    print(x)
    print("-----")