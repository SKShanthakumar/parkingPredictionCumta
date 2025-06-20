from dotenv import load_dotenv
import os
import mysql.connector

load_dotenv()

# MySQL setup
try:
    mysql_db = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        port=os.getenv("DB_PORT")
    )
    cursor = mysql_db.cursor()
    # cursor.execute("SELECT * FROM availability")
    # rows = cursor.fetchall()
    # for row in rows:
    #     print(row)
    print("MySQL connection established successfully.")

except mysql.connector.Error as e:
    print(f"MySQL connection error: {e}")
    