import json
import asyncio
from aiokafka import AIOKafkaProducer

from faker import Faker

# Create a Faker instance
fake = Faker()

topic = 'test'

def serializer(value):
    return json.dumps(value).encode()

async def produce():
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:29092',
        value_serializer=serializer,
        compression_type="gzip")

    await producer.start()
    name = fake.name()
    data = {"id": 4, "name": name, "song": "Seek and Destroy"}
    await producer.send(topic, data)
    name = fake.name()
    data = {"id": 1, "name": name, "song": "Ball and Biscuit"}
    await producer.send(topic, data)
    await producer.stop()

loop = asyncio.get_event_loop()
result = loop.run_until_complete(produce())


