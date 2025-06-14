import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import os
import mysql.connector
from dateutil.parser import parse

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
    print("MySQL connection established successfully.")

except mysql.connector.Error as e:
    print(f"MySQL connection error: {e}")
    exit(1)

now = datetime.now()
timestamp = datetime.now(ZoneInfo("Asia/Kolkata")).replace(microsecond=0).replace(tzinfo=None)

# Fetching ticket share data from the API
try:
    url = os.getenv("TICKET_SHARE_API")
    response = requests.get(url)
    data = response.json()

    insert_query = """
    INSERT INTO ticket_share (
        Timestamp, TotalTickets, SVC, NCMCcard, MobileQR, StaticQR, PaperQR, PaytmQR, WhatsAppQR,
        PhonePeQR, TotalQrCount, PromotionalRideQR, Tripcard, TouristCard, Token, GroupCard,
        TotalQR, Cards, TotalCards, RedBusQR, RapidoQR, JusPayQR, ONDCQR
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """
    record = [
        timestamp,
        data.get('totalTickets'),
        data.get('noOfSVC'),
        data.get('noOfNCMCcard'),
        data.get('noOfMobileQR'),
        data.get('noOfStaticQR'),
        data.get('noOfPaperQR'),
        data.get('noOfPaytmQR'),
        data.get('noOfWhatsAppQR'),
        data.get('noOfPhonePeQR'),
        data.get('totalQrCount'),
        data.get('noOfPromotionalRideQR'),
        data.get('noOfTripcard'),
        data.get('noOfTouristCard'),
        data.get('noOfToken'),
        data.get('noOfGroupCard'),
        data.get('noOfTotal_QR'),
        data.get('noOfCards'),
        data.get('totalCards'),
        data.get('noOfRedBusQR'),
        data.get('noOfRapidoQR'),
        data.get('noOfJusPayQR'),
        data.get('noOfONDCQR')
    ]

    cursor.execute(insert_query, record)
    mysql_db.commit()
    print("Ticket share data inserted successfully.")

except mysql.connector.Error as e:
    print(f"MySQL Error during ticket_share insert: {e}")
    mysql_db.rollback()

except Exception as e:
    print(f"Error fetching/parsing ticket share data: {e}")


# Fetching hourly passenger data from the API
try:
    url = os.getenv("HOURLY_DATA_API")
    response = requests.get(url)
    data = response.json()

    insert_query = """
    INSERT INTO passenger_hourly_data (
        Timestamp, Hour, Total, StoreValueCard, Token, TripCard, TouristCard, GroupTicket,
        NCMCCard, PaperQR, MobileQR, StaticQR, WhatsappQR, PaytmQR, PhonepeQR,
        ONDCRedBusQR, ONDCRapidoQR, ONDCNammaYatriQR
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """

    records = [[timestamp, parse(time)] for time in data.get('categories')]
    for doc in data.get('series'):
        for i in range(len(records)):
            records[i].append(doc.get('data')[i])

    # Bulk insert all records
    if records:
        cursor.executemany(insert_query, records)
        mysql_db.commit()
        print(f"Successfully inserted {len(records)} records into passenger_hourly_data table")
    else:
        print("No records to insert")

except mysql.connector.Error as e:
    print(f"MySQL Error during bulk insert: {e}")
    mysql_db.rollback()
    # Try individual inserts if bulk insert fails
    print("Attempting individual inserts...")
    successful_inserts = 0
    failed_inserts = 0
    for i, record in enumerate(records):
        try:
            cursor.execute(insert_query, record)
            mysql_db.commit()
            successful_inserts += 1
        except mysql.connector.Error as individual_error:
            print(f"Failed to insert record {i+1}: {individual_error}")
            print(f"Record data: {record}")
            failed_inserts += 1
            mysql_db.rollback()
    print(f"Successful inserts: {successful_inserts}, Failed inserts: {failed_inserts}")

except Exception as e:
    print(f"Error fetching/parsing hourly passenger data: {e}")

# Fetching station data from the API
try:
    url = os.getenv("STATION_DATA_API")
    response = requests.get(url)
    data = response.json()

    insert_query = """
    INSERT INTO station_data (
        Timestamp, Station, Line, Total, StoreValueCard, Token, TripCard, TouristCard, GroupTicket,
        NCMCCard, PaperQR, MobileQR, StaticQR, WhatsappQR, PaytmQR, PhonepeQR,
        ONDCRedBusQR, ONDCRapidoQR, ONDCNammaYatriQR
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """

    record1 = [[timestamp, station, 1] for station in data[0].get('categories')]
    record2 = [[timestamp, station, 2] for station in data[1].get('categories')]

    for doc in data[0].get('series'):
        for i in range(len(record1)):
            record1[i].append(doc.get('data')[i])
    
    for doc in data[1].get('series'):
        for i in range(len(record2)):
            record2[i].append(doc.get('data')[i])

    records = record1 + record2

    # Bulk insert all records
    if records:
        cursor.executemany(insert_query, records)
        mysql_db.commit()
        print(f"Successfully inserted {len(records)} records into station_data table")
    else:
        print("No records to insert")

except mysql.connector.Error as e:
    print(f"MySQL Error during bulk insert: {e}")
    mysql_db.rollback()
    # Try individual inserts if bulk insert fails
    print("Attempting individual inserts...")
    successful_inserts = 0
    failed_inserts = 0
    for i, record in enumerate(records):
        try:
            cursor.execute(insert_query, record)
            mysql_db.commit()
            successful_inserts += 1
        except mysql.connector.Error as individual_error:
            print(f"Failed to insert record {i+1}: {individual_error}")
            print(f"Record data: {record}")
            failed_inserts += 1
            mysql_db.rollback()
    print(f"Successful inserts: {successful_inserts}, Failed inserts: {failed_inserts}")

except Exception as e:
    print(f"Error fetching/parsing hourly passenger data: {e}")

finally:
    # Close the MySQL connection
    if cursor:
        cursor.close()
    if mysql_db:
        mysql_db.close()
    print("MySQL connection closed.")