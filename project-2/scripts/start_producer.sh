vagrant ssh -c "
docker start vagrant_kafka_1 vagrant_zookeeper_1 cassandra;
cd /vagrant;
python3 src/data-producer.py
" | tee ../output.txt

