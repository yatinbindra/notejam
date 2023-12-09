from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType
import os




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


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 pyspark-shell'



def session():

    spark = SparkSession.builder.appName(appName).master("local[*]").getOrCreate()
    # spark.sparkContext.setLogLevel("warn")

    return spark


def read_CSV():

    csv_Data = spark.readStream.format('csv') \
                    .schema(SCHEMA) \
                    .option('header',True) \
                    .option('multiLine', True) \
                    .load('../taskDetails') \
                
    return csv_Data


def write_to_console(csv_Data):   

    csv_Data.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate","False")\
        .start() \
        .awaitTermination() 


def write_to_Kafka(csv_Data):

    csv_Data.withColumn("value", to_json(struct("*"))).select("value")\
        .writeStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)\
        .option("topic", KAFKA_TOPIC)\
        .option("checkpointLocation", "/tmp/checkpoint03")\
        .start() \
        .awaitTermination()





if __name__ == '__main__':
    
    spark = session()
    csv_Data = read_CSV()
    write_to_Kafka(csv_Data)
    # write_to_console(csv_Data)

