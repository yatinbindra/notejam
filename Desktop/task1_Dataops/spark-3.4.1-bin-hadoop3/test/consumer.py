from pyspark.sql import SparkSession
from delta.tables import *
from delta import *
from delta.pip_utils import configure_spark_with_delta_pip
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType



KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "quickstart-events"
appName = "sample"

SCHEMA = StructType([
    StructField("Date/Time", StringType(),True),                
    StructField("LV ActivePower (kW)", StringType(),True),        
    StructField("Wind Speed (m/s)", StringType(),True),       
    StructField("Theoretical_Power_Curve (KWh)", StringType(),True),    
    StructField("Wind Direction (°)", StringType(),True)                 
])



def session():

    builder = spark = SparkSession.builder \
    .master('local[*]') \
    .config("spark.driver.memory", "15g") \
    .appName(appName)\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    my_packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"]

    spark = configure_spark_with_delta_pip(builder,extra_packages=my_packages).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark



def read_from_kafka(spark):

    kafka_data = spark.readStream.format("kafka") \
                      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                      .option("subscribe", KAFKA_TOPIC) \
                      .option("startingOffsets", "earliest") \
                      .load()
    return kafka_data


def transformation(kafka_data):
    json_to_string = kafka_data.withColumn("value",from_json(col("value").cast(StringType()),SCHEMA)).select(("value.*"))

    delta_formate = json_to_string.select(to_date(("Date/Time"), "dd MM yyyy HH:mm").alias("signal_date"),
                   to_timestamp("Date/Time","dd MM yyyy HH:mm").alias("signal_ts"),
                   to_date(current_timestamp()).alias("create_date"),
                   to_timestamp(current_timestamp(),"HH:mm:ss").alias("creare_ts"),
                   create_map(
                   lit("LV ActivePower (kW)"),col("LV ActivePower (kW)"),
                   lit("Wind Speed (m/s)"),col("Wind Speed (m/s)"),
                   lit("Theoretical_Power_Curve (KWh)"),col("Theoretical_Power_Curve (KWh)"),
                   lit("Wind Direction (°)"),col("Wind Direction (°)")).alias("signals"))

    return delta_formate
    

def write_to_console(delta_formate):
    delta_formate\
        .writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate",False) \
        .start()\
        .awaitTermination()


def write_to_delta(delta_formate):

    delta_formate\
        .writeStream \
        .format("delta") \
        .option("truncate",False)\
        .option("checkpointLocation", "./test/checkpoint") \
        .start("./test/kafka-events")\
        .awaitTermination()



if __name__ == '__main__':
    spark = session()
    kafka_data = read_from_kafka(spark)
    delta_formate = transformation(kafka_data)
    # write_to_console(delta_formate)
    write_to_delta(delta_formate)