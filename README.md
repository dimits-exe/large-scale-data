# Large Scale Data Management

This repository consists of two projects: 
* A Java [Hadoop map-reduce application](https://github.com/dimits-exe/large-scale-data/tree/master/map-reduce) which:
  - Computes the occurences of each word for a large file
  - Computes spotify song statistics for each country and month
  
* A [Hadoop SPARK-Cassandra application](https://github.com/dimits-exe/large-scale-data/tree/master/project-2) which:
   - Generates a configurable stream of test data, posting them to a Kafka cluster
   - Reads, preprocesses and combines the stream data with static data using SPARK
   - Periodically posts them to a Cassandra cluster
   - Performs queries using CQL on the Cassandra cluster
  
