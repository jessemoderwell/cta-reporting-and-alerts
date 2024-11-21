# Imports
import mysql.connector
import time
from datetime import datetime, timedelta
import sys
import os
from dotenv import load_dotenv

import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

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
SERVICE_EMAIL = os.getenv('SERVICE_EMAIL')
SERVICE_EMAIL_PASSWORD = os.getenv('SERVICE_EMAIL_PASSWORD')

def get_new_subscriber_alerts(cursor, hour_window):
    since_time = (datetime.now() - timedelta(hours=hour_window + 6)).strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute(f"""
    select n.notification_id, s.train_line, s.station, n.end_station, u.email, n.num_delays
    from station_subscriptions s
    inner join delay_notifications n on (s.train_line, s.station) = (n.train_line, n.station)
    inner join users u on s.user_id = u.user_id
    where n.notification_sent = False
    and n.alert_time >= '{since_time}'
    """)

    return cursor.fetchall()

def mark_notifications_as_sent(cursor, mydb, notification_id):
    cursor.execute(f"""
    update delay_notifications
    set notification_sent = True
    where notification_id = {notification_id}
    """)

    mydb.commit()

def send_notification_emails(SERVICE_EMAIL, SERVICE_EMAIL_PASSWORD, subscriber_notifications):
    for notification in subscriber_notifications:
        notification_id, train_line, station = notification[:3]
        end_station, recipient_email, num_delays = notification[3:]

        sender_email = SERVICE_EMAIL
        receiver_email = recipient_email
        password = SERVICE_EMAIL_PASSWORD

        message = MIMEMultipart("alternative")
        message["Subject"] = f"{train_line} train(s) at {station} are delayed toward {end_station}"
        message["From"] = SERVICE_EMAIL
        message["To"] = SERVICE_EMAIL

        # Create the plain-text and HTML version of your message
        text = f"""
        There have been {num_delays} delayed {train_line} train(s) at {station} toward {end_station}
        in the last hour
        """

        # Turn these into plain/html MIMEText objects
        part1 = MIMEText(text, "plain")
        # part2 = MIMEText(html, "html")

        # Add HTML/plain-text parts to MIMEMultipart message
        # The email client will try to render the last part first
        message.attach(part1)
        # message.attach(part2)

        # Create secure connection with server and send email
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(
                sender_email, receiver_email, message.as_string()
            )
            
        mark_notifications_as_sent(mycursor, mydb, notification_id)

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

    subscriber_notifications = get_new_subscriber_alerts(mycursor, 1)
    print(subscriber_notifications)
    if subscriber_notifications:
        send_notification_emails(SERVICE_EMAIL, SERVICE_EMAIL_PASSWORD, subscriber_notifications)

    mydb.close()

    time.sleep(15)




