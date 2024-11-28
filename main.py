import time

import requests
import json
import random

from confluent_kafka import Producer
# from kafka import  KafkaProducer

KAFKA_BOOTSTRAP_SERVER = ['localhost:9092']
KAFKA_TOPIC = "user_information"
STREAMING_DURATION = 120
PAUSE_INTERVAL = 10

def get_data():
    results = requests.get("https://randomuser.me/api/")
    result = results.json()
    result = result['results'][0]
    return result

def format_data(result):
    data = {}
    # data['id'] = data['login']['uuid']
    data['gender'] = random.choice(['female', 'male'])
    data['title'] = random.choice(['Mr', 'Mrs'])
    data['first_name'] = result['name']['first']
    data['last_name'] = result['name']['last']
    data['city'] = result['location']['city']
    data['country'] = result['location']['country']
    data['postcode'] = result['location']['postcode']
    data['longitude'] = result['location']['coordinates']['longitude']
    data['latitude'] = result['location']['coordinates']['latitude']
    data['email'] = result['email']
    data['age'] = result['registered']['age']
    data['phone'] = result['phone']

    return  data

"""KAFKA INITIALISATION AND DEVELOPMENT"""

def configure_kafka(servers=KAFKA_BOOTSTRAP_SERVER):
    """Create and return a kafka producer instance"""
    settings = {
        'bootstrap.servers': ','.join(servers),
        # 'client.id': 'producer_instance'
    }
    return Producer(settings)


def publish_to_kafka(producer, topic, data):
    """Publish data to kafka"""
    producer.produce(topic, value=json.dumps(data).encode('utf-8'), callback=delivery_status)
    producer.flush()


def delivery_status(err, msg):
    """Reports the delivery message"""
    if err is not None:
        print("Message delivery failed.", err)
    else:
        print("Message delivered to", msg.topic())


def initiate_stream():
    """Initiate data stream to kafka"""
    kafka_producer = configure_kafka()
    for _ in range(STREAMING_DURATION // PAUSE_INTERVAL):
        raw_data = get_data()
        kafka_data = format_data(raw_data)
        publish_to_kafka(kafka_producer, KAFKA_TOPIC, kafka_data)
        time.sleep(PAUSE_INTERVAL)


if __name__ == "__main__":
    initiate_stream()