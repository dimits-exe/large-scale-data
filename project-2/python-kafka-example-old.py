from pykafka import KafkaClient
import threading
import json

KAFKA_HOST = "localhost:29092" # Or the address you want

client = KafkaClient(hosts = KAFKA_HOST)
topic = client.topics["test"]

import json 
#from confluent_kafka import Producer
from kafka import Producer

p = Producer({'bootstrap.servers': KAFKA_HOST})

with topic.get_sync_producer() as producer2:
    for i in range(10):
        data = { 'tag ': 'blah',
            'name' : 'sam',
            'index' : i,
            'score':
                {
                    'row1': 100,
                    'row2': 200
                }
        }

    producer.produce('test', data.encode('utf-8'))
