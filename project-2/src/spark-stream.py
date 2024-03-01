from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)
from pyspark.sql.functions import from_json, col


# spark initialization
spark = (
    SparkSession.builder.appName("SSKafka")
    .config(
        "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# spotify songs
song_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("file:////vagrant/data/spotify-songs.csv")
    .withColumnRenamed("name", "song_name")
    .cache()
)

song_df.printSchema()

# request from kafka consumer
request_schema = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("song", StringType(), False),
        StructField("time", StringType(), False),
    ]
)

request_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "test")
    .option("startingOffsets", "latest")
    .load()
    .withColumnRenamed("name", "user_name")
)

request_df = (
    request_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), request_schema).alias("data"))
    .select("data.*")
)

request_df.printSchema()

# Joining the DataFrames
joined_df = request_df.join(
    song_df, request_df.song == song_df.song_name, "inner"
).drop("song_name")

# Printing the schema of the joined DataFrame
joined_df.printSchema()

# Specify the output mode and format
query = (
    joined_df.writeStream.outputMode("update")
    .format("console")
    .option("truncate", False)
    .start()
)

# Wait for the query to terminate
query.awaitTermination()
