
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,LongType,IntegerType,FloatType,StringType
from pyspark.sql.functions import split,from_json,col

songSchema = StructType([
                StructField("id", IntegerType(),False),
                StructField("name", StringType(),False),
                StructField("song", StringType(),False)
            ])

spark = SparkSession.builder.appName("SSKafka").config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:29092").option("subscribe", "test").option("startingOffsets", "latest").load() 

sdf = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), songSchema).alias("data")).select("data.*")

def writeToCassandra(writeDF, _):
  writeDF.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="records", keyspace="spotify").save()

result = None
while result is None:
    try:
        # connect
        result = sdf.writeStream.option("spark.cassandra.connection.host","localhost:9042").foreachBatch(writeToCassandra).outputMode("update").start().awaitTermination()
    except:
         pass

