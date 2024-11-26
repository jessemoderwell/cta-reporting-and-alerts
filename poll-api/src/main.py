import asyncio
import aiohttp
from kafka import KafkaProducer
import json
import os
from dotenv import load_dotenv
import time

time.sleep(10)

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), '../../.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

CTA_API_KEY = os.getenv('CTA_API_KEY')
LOCATIONS_API_ENDPOINT = os.getenv('LOCATIONS_API_ENDPOINT')

producer = KafkaProducer(
    bootstrap_servers='kafka:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

async def fetch_latest_etas(session, train_line):
    params = {'rt': train_line, 'key': CTA_API_KEY, 'outputType': 'JSON'}
    async with session.get(LOCATIONS_API_ENDPOINT, params=params) as response:
        return await response.json()

async def poll_api_and_send(train_lines):
    while True:
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_latest_etas(session, line) for line in train_lines]
            results = await asyncio.gather(*tasks)
            
            for data in results:
                producer.send('train-etas', data)
                print(f'Sent train ETA data for {data["ctatt"]["route"][0]["@name"]}')
        
        await asyncio.sleep(30)

if __name__ == '__main__':
    train_lines = ['red', 'blue', 'brn', 'g', 'org']  # Add desired train lines here
    asyncio.run(poll_api_and_send(train_lines))