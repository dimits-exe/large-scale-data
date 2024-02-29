import json
import random
import asyncio
import csv
import datetime
import time

from aiokafka import AIOKafkaProducer
from faker import Faker


INTERVAL_SECS = 30
MESSAGE_LIMIT = 20
SONG_FILE_PATH = "data/spotify-songs.csv"
TOPIC = "test"


def serializer(value):
    return json.dumps(value).encode()


def generate_record(id: int, name: str, song: str) -> dict:
    return {
        "id": id,
        "name": name,
        "song": song,
        "time": datetime.datetime.now().strftime("%Y-%m-%d %I:%M"),
    }


def generate_names(num_names: int) -> list:
    fake = Faker()
    fake_names = [fake.name() for _ in range(num_names - 1)]
    fake_names.insert(0, "Tsirmpas Dimitris")
    return fake_names


def generate_songs(song_file_path: str) -> list:
    with open(song_file_path, "r") as file:
        reader = csv.reader(file, delimiter=",")
        songs = [row[0] for row in reader]
        random.shuffle(songs)
        return songs


async def produce():
    producer = AIOKafkaProducer(
        bootstrap_servers="localhost:29092",
        value_serializer=serializer,
        compression_type="gzip",
    )

    names = generate_names(50)
    songs = generate_songs(SONG_FILE_PATH)

    await producer.start()
    print("Data stream open. Records sent:")

    num_messages_sent = 0
    while num_messages_sent < MESSAGE_LIMIT:
        data = generate_record(
            random.randint(0, 99999),
            names[num_messages_sent],
            songs[num_messages_sent],
        )
        print(data)

        await producer.send(TOPIC, data)
        num_messages_sent += 1
        time.sleep(INTERVAL_SECS)

    await producer.stop()


loop = asyncio.get_event_loop()
result = loop.run_until_complete(produce())
