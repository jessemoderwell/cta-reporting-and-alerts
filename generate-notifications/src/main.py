# Imports
import mysql.connector
import time
from datetime import datetime, timedelta
import sys
import os
from dotenv import load_dotenv

# setting path
# sys.path.append('../..')

# Load .env only if running locally
dotenv_path = os.path.join(os.path.dirname(__file__), '../../.env')  # Adjust path if needed
if os.path.exists(dotenv_path):
    print("Loading environment variables from .env file")
    load_dotenv(dotenv_path)
else:
    print("Running in a container; environment variables loaded externally.")

# Access environment variables
DATABASE_NAME = os.getenv('DATABASE_NAME')
DATABASE_USER = os.getenv('DATABASE_USER')
DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')
print(DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD)

def get_recent_notifications(hours_window, cursor):
    since_time = (datetime.now() - timedelta(hours=hours_window + 6))
    since_time_string = since_time.strftime("%Y-%m-%d %H:%M:%S")
    print(since_time_string)

    cursor.execute(f"""
    select train_line, station, end_station 
    from delay_notifications
    where alert_time >= "{since_time_string}"
    """)

    result = cursor.fetchall()

    print(result)

    # Returns tuples with train_line, station, and end_station for exist notifications
    return set([(record[0], record[1], record[2]) for record in result])

def create_new_notifications(cursor, delay_threshold, hours_window):
    since_time = (datetime.now() - timedelta(hours=hours_window + 6))
    since_time_string = since_time.strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute(f"""
    select train_line, next_station, end_station, count(delay_in_minutes), sum(delay_in_minutes)
    from train_delays
    where latest_eta >= "{since_time_string}"
    and delay_in_minutes >= {delay_threshold}
    group by train_line, next_station, end_station
    having count(delay_in_minutes) >= 2
    """)

    result = cursor.fetchall()

    for x in result:
        print(x)

    new_notifications = []
    curr_notifications = get_recent_notifications(3, cursor)
    print(curr_notifications)
    current_time = datetime.now()
    current_time_cst = (current_time - timedelta(hours=6)).strftime("%Y-%m-%d %H:%M:%S")

    for delay in result:
        train_line, station, end_station = delay[:3]

        if (train_line, station, end_station) in curr_notifications:
            continue

        insert_query = f"""
        insert into delay_notifications 
        values (null, '{delay[0]}', '{delay[1]}',
        '{delay[2]}', {delay[3]}, '{current_time_cst}', False)
        """
        print(insert_query)

        try:
            # Execute your transaction here
            cursor.execute("START TRANSACTION;")
            cursor.execute(insert_query)
            mydb.commit()
            print("Transaction committed successfully.")
        except mysql.connector.Error as err:
            mydb.rollback()
            print(err)
            print("Transaction failed. Rolled back.")

while True:
    # Initialize mysql connection
    mydb = mysql.connector.connect(
    host="mysql",
    port=3306,
    user=DATABASE_USER,
    password=DATABASE_PASSWORD,
    database=DATABASE_NAME
    )

    mycursor = mydb.cursor()

    create_new_notifications(mycursor, 1, 1)

    mydb.close()
    time.sleep(15)
        
        