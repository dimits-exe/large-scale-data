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
SONG_FILE_PATH = "../data/spotify_songs.csv"
TOPIC = "songs"


def serializer(value):
    return json.dumps(value).encode()


def generate_record(id: int, name: str, song: str) -> dict:
    return {"id": id, "name": name, "song": song, "time": datetime.datetime.now()}


def generate_rand_record(rand_names: list[str], rand_songs: list[str]) -> dict:
    name = random.choice(rand_names)
    song = random.choice(rand_songs)
    id = random.randint(0, 9999999)

    return generate_record(id, name, song)


def generate_names(num_names: int) -> list[str]:
    fake = Faker()
    fake_names = [fake.name() for _ in range(num_names) - 1]
    fake_names.insert(0, "Tsirmpas Dimitris")
    return fake_names


def generate_songs(song_file_path: str) -> list[str]:
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

    await producer.start()

    names = generate_names(50)
    songs = generate_songs(SONG_FILE_PATH)

    num_messages_sent = 0
    while num_messages_sent < MESSAGE_LIMIT:
        data = generate_rand_record(names, songs)
        await producer.send(TOPIC, data)
        num_messages_sent += 1
        time.sleep(INTERVAL_SECS)

    await producer.stop()


loop = asyncio.get_event_loop()
result = loop.run_until_complete(produce())
