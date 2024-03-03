SELECT song
FROM spotify.listening_history
WHERE name = 'Dimitris Tsirmpas'
AND date = '2024-03-03'
AND hour=10;

SELECT name, date, hour, avg(danceability), avg(tempo), avg(acousticness), avg(tempo)
FROM spotify.listening_history
WHERE name = 'Dimitris Tsirmpas'
AND date = '2024-03-03'
AND hour = 10
GROUP BY hour;