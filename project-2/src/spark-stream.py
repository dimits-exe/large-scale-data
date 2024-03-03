from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)
from pyspark.sql.functions import from_json, col, udf


DEBUG = False
FLUSH_INTERVAL_SECS = 30

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
    spark
    .read
    .option("header", True)
    .option("inferSchema", True)
    .csv("file:////vagrant/data/spotify-songs.csv")
    .withColumnRenamed("name", "song_name") # disambiguate user name and song name
    .withColumnRenamed("key", "musical_key") # avoid cql reserved word "key"
    .cache() # cache dataset as the file is immutable
)

print("Song dataframe schema:")
song_df.printSchema()

# request from kafka consumer

@udf(returnType=IntegerType())
def derive_hour(datetime_string: str) -> int:
    _, time = datetime_string.split(" ")
    return int(time[:2])


@udf(returnType=StringType())
def derive_date(datetime_string: str) -> str:
    return datetime_string.split(" ")[0]


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
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), request_schema).alias("data"))
    .select("data.*")
    # add columns needed for the partition key
    .withColumn("hour", derive_hour(col("time")))
    .withColumn("date", derive_date(col("time")))
)

print("Request dataframe schema:")
request_df.printSchema()

# Joining the DataFrames
joined_df = request_df.join(
    song_df, request_df.song == song_df.song_name, "inner"
).drop("song_name")

# Printing the schema of the joined DataFrame
print("Final dataframe schema:")
joined_df.printSchema()


# Specify the output mode and format
def writeToCassandra(writeDF, _):
    print(f"Flushing {writeDF.count()} records to database...")
    (writeDF
    .write
    .format("org.apache.spark.sql.cassandra")
    .mode("append")
    .options(table="listening_history", keyspace="spotify")
    .save())


if DEBUG:   
    debug_query = (
        joined_df
        .writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .start()
        .awaitTermination()
    )
else:
    # retry until connection is established
    result = None
    while result is None:
        try:
            # connect
            cassandra_query = (
                joined_df.writeStream.option(
                    "spark.cassandra.connection.host", "localhost:9042"
                )
                .foreachBatch(writeToCassandra)
                .outputMode("update")
                .trigger(processingTime=f"{FLUSH_INTERVAL_SECS} seconds")
                .start()
                .awaitTermination()
            )
        except:
            pass
