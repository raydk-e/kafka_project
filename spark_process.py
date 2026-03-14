from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("WeatherProcessor")\
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.postgresql:postgresql:42.7.2")\
    .getOrCreate()

schema = StructType([
    StructField("city", StringType()),
    StructField("temp", IntegerType())
])

df =  spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "weather-data")\
    .option("startingOffsets", "latest")\
    .load()
parsed_df = df.selectExpr("CAST(value AS STRING)")\
        .select(from_json(col("value"),schema).alias("data"))\
        .select("data.*")
def write_to_postgres(batch_df,batch_id):
    batch_df.write\
        .format("jdbc")\
        .option("url", "jdbc:postgresql://localhost:5432/weather_db")\
        .option("dbtable", "city_temps")\
        .option("user", "postgres")\
        .option("password","password")\
        .option("driver","org.postgresql.Driver")\
        .mode("append")\
        .save()
query = parsed_df.writeStream\
    .foreachBatch(write_to_postgres)\
    .option("checkpointLocation", "/home/deepak/kafka_project/checkpoints") \
    .start()
query.awaitTermination()
        