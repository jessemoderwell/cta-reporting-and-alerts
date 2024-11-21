from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import time
from datetime import datetime
import os
from dotenv import load_dotenv
import requests

# Load .env only if running locally
dotenv_path = os.path.join(os.path.dirname(__file__), '../../.env')  # Adjust path if needed
if os.path.exists(dotenv_path):
    print("Loading environment variables from .env file")
    load_dotenv(dotenv_path)
else:
    print("Running in a container; environment variables loaded externally.")

time.sleep(10)

# Access environment variables
CTA_API_KEY = os.getenv('CTA_API_KEY')

def fetch_latest_etas(train_line: str):
    params = {'rt':train_line,
         'key': CTA_API_KEY,
         'outputType':'JSON'}
    
    response = requests.get("https://lapi.transitchicago.com/api/1.0/ttpositions.aspx", params=params)
    
    return response.json()

try:
    admin_client = KafkaAdminClient(
        bootstrap_servers="kafka:29092",  # Replace with your Kafka broker address
    #     client_id="my-admin-client"
    )

    topic_name = "train-etas"
    num_partitions = 1
    replication_factor = 1

    new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

    admin_client.create_topics([new_topic])
except Exception as e:
    print(Exception)

producer = KafkaProducer(bootstrap_servers='kafka:29092',
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def poll_api_and_send(train_line):
    while True:
        latest_data = fetch_latest_etas(train_line)
        producer.send('train-etas', latest_data)
        print('sent latest train eta data')
        
        time.sleep(30)

poll_api_and_send('red')