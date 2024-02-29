from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,LongType,IntegerType,FloatType,StringType
from pyspark.sql.functions import split,from_json,col

spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:29092") \
  .option("subscribe", "test") \
  .option("startingOffsets", "latest") \
  .load() 

df.writeStream \
  .outputMode("update") \
  .format("console") \
  .option("truncate", False) \
  .start() \
  .awaitTermination()
