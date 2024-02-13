vagrant ssh -c "
cd /vagrant/part2;
docker cp universal_top_spotify_songs.csv namenode:/;
docker exec namenode hdfs dfs -put universal_top_spotify_songs.csv /user/hdfs/input/;
cd map-reduce-spotify;
mvn clean install;
docker cp /vagrant/part2/map-reduce-spotify/target/hadoop-spotify-1.0-SNAPSHOT-jar-with-dependencies.jar namenode:/;
docker exec namenode hdfs dfs -rm -r /user/hdfs/output/;
docker exec namenode hadoop jar /hadoop-spotify-1.0-SNAPSHOT-jar-with-dependencies.jar namenode:/;
docker exec namenode hdfs dfs -text /user/hdfs/output/part-r-00000 | head -100;
" | tee output.txt

