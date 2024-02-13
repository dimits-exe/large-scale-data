vagrant ssh -c "
wget \"https://raw.githubusercontent.com/amephraim/nlp/master/texts/J.%20K.%20Rowling%20-%20Harry%20Potter%201%20-%20Sorcerer's%20Stone.txt\" -O harry_potter.txt;
docker exec namenode hdfs dfs -put harry_potter.txt /user/hdfs/input/;
docker cp \"harry_potter.txt\" namenode:/;
cd /vagrant/hadoop-mapreduce-examples/; 
mvn clean install; 
docker cp /vagrant/hadoop-mapreduce-examples/target/hadoop-map-reduce-examples-1.0-SNAPSHOT-jar-with-dependencies.jar namenode:/;
docker exec namenode hdfs dfs -rm -r /user/hdfs/output/
docker exec namenode hadoop jar /hadoop-map-reduce-examples-1.0-SNAPSHOT-jar-with-dependencies.jar;
docker exec namenode hdfs dfs -text /vagrant/hdfs/output/part-r-00000 | head -100;
" | tee output.txt
