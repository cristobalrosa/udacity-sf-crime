import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 pyspark-shell'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 pyspark-shell'

# TODO Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", TimestampType(), True),
    StructField("call_date", TimestampType(), True),
    StructField("offense_date", TimestampType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", TimestampType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True),
])

radio_code_schema = StructType([
    StructField("disposition_code", StringType(), True),
    StructField("description", StringType(), True)
])


def configure_logging(spark):
    """Configures logging so it is quiet"""
    log4j = spark._jvm.org.apache.log4j
    log4j.LogManager.getLogger("org").setLevel(log4j.Level.ERROR)
    log4j.LogManager.getLogger("org.apache.spark.sql.execution.streaming.MicroBatchExecution").setLevel(log4j.Level.INFO)
    log4j.LogManager.getLogger("akka").setLevel(log4j.Level.ERROR)


def run_spark_job(spark):
    configure_logging(spark)
    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    # Ref: https://spark.apache.org/docs/2.2.0/structured-streaming-kafka-integration.html
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "udacity.sf.police.crime.v2") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    # kafka_df.writeStream.format("console").outputMode("append").start()

    service_table = kafka_df \
        .select(psf.from_json(psf.col('value'), schema).alias("DF")) \
        .select("DF.*")

    # service_table.writeStream.format("console").outputMode("append").start()

    # I have seen there ara some rows with null values in both, original_crime_type
    # and disposition, so I will filter them out.

    service_table_non_nulls = service_table.na.drop(subset=["original_crime_type_name", "disposition"])

    # TODO select original_crime_type_name and disposition
    # I'm using pst.to_timestamp to convert the string timestamp into a timestamp object so we can use it later
    # to do watermarking and windowed aggregations.

    distinct_table = service_table_non_nulls.select("original_crime_type_name",
                                                    "disposition",
                                                    psf.to_timestamp("call_date_time").alias(
                                                        "call_date_time_ts")).distinct()
    # distinct_table.writeStream.format("console").outputMode("append").start()

    # count the number of original crime type
    # Nice blog about watermarking
    # https://databricks.com/blog/2017/05/08/event-time-aggregation-watermarking-apache-sparks-structured-streaming.html
    # Discarding events that arrive more than 10 minutes late. I don't want to set a huge watermark to avoid having
    # memory issues
    agg_df = distinct_table \
        .select("original_crime_type_name", "disposition", "call_date_time_ts") \
        .withWatermark("call_date_time_ts", "10 minutes") \
        .groupBy("original_crime_type_name",
                 psf.window("call_date_time_ts", "10 minutes", "5 minutes"),
                 "disposition"  # Including this field so I can run the aggregation later.
                 ) \
        .count()

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream

    query = agg_df \
        .writeStream \
        .queryName("Original Crime Type Count Aggregation")\
        .trigger(processingTime="30 seconds") \
        .format('console') \
        .option("truncate", "false") \
        .start()

    # TODO attach a ProgressReporter
    #query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "./radio_code.json"
    radio_code_df = spark.read. \
        option("multiline", "true"). \
        json(radio_code_json_filepath, radio_code_schema)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    radio_code_df.printSchema()
    # TODO join on disposition column
    # Nice blog on joins: https://luminousmen.com/post/introduction-to-pyspark-join-types
    # In this case, if we use an inner join unless the disposition is on the radio dataframe we
    # wont see any results, so I will be using a left join, because I want to see the data on the agg even
    # if there is no a matching disposition
    join_query = agg_df.join(
        radio_code_df,
        on='disposition',
        how='left'
    )

    query_join = join_query \
        .writeStream \
        .queryName("Join with radio codes")\
        .trigger(processingTime="30 seconds") \
        .format('console') \
        .option("truncate", "false") \
        .start()
    query_join.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.default.parallelism", "4") \
        .config("spark.sql.shuffle.partitions", "5") \
        .getOrCreate()

    conf = spark.sparkContext.getConf()
    print("================================================================>")
    for value in conf.getAll():
        print(value)
    print("================================================================>")

    logger.info("Spark started")
    run_spark_job(spark)

    spark.stop()
