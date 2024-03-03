CREATE KEYSPACE IF NOT EXISTS spotify 
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

CREATE TABLE IF NOT EXISTS spotify.listening_history (
    id int,
    name text,
    song text,
    time text,
    hour int, -- seperate hour column for partition key
    date text, -- seperate date column for partition key
    artists text,
    duration_ms int,
    album_name text,
    album_release_name text,
    album_release_date text,
    danceability float,
    energy float,
    musical_key int, -- originally "key" in the csv, which is a reserved word in cql
    loudness float,
    mode float,
    speechiness float,
    acousticness float,
    instrumentalness float,
    liveness float,
    valence float,
    tempo float,
    PRIMARY KEY ((name, date, hour), time)
) WITH CLUSTERING ORDER BY (time DESC);