from pyspark import *
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
from pyspark.sql.functions import *

appName = "sample"

def session():

    builder = spark = SparkSession.builder \
    .master('local[*]') \
    .config("spark.driver.memory", "15g") \
    .appName(appName)\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    my_packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"]

    spark = configure_spark_with_delta_pip(builder,extra_packages=my_packages).getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    return spark



def task1(spark):
    print("result from task 1") 
    read_from_delta = spark \
        .read.format("delta") \
        .load("./test/kafka-events")
    read_from_delta.show(20,False)
    return read_from_delta



def task2(read_from_delta):
    print("result of task 2")
    read_from_delta\
        .groupBy(("signal_date"))\
        .agg(count_distinct("signal_ts").alias("datapoints per day"))\
        .sort(asc("signal_date"))\
        .show(20,False)
    

def task3(read_from_delta):
    print("result of task 3")

    resultFromTask3 = read_from_delta.groupBy(["signal_date",hour("signal_ts").alias("signal_ts")])\
         .agg(avg("signals.LV ActivePower (kW)").alias("ActivePower"),
              avg("signals.Wind Speed (m/s)").alias("WindSpeed"),
              avg("signals.Theoretical_Power_Curve (KWh)").alias("Theoretical_Power_Curve"),
              avg("signals.Wind Direction (°)").alias("WindDirection"))\
              .orderBy(["signal_date","signal_ts"])
    
    # rows = read_from_delta.count()
    
    resultFromTask3.show(20,False)
    return resultFromTask3



def task4(resultFromTask3):
    print("result of task 4")

    resultFromTask4 =  resultFromTask3.select(col("*"),when(resultFromTask3.ActivePower < "200","low")
                .when(resultFromTask3.ActivePower < "600","Medium")
                .when(resultFromTask3.ActivePower < "1000","High")
                .when(resultFromTask3.ActivePower > "1000","Exceptional")
                .alias("generation_indicator"))
    resultFromTask4.show(25,False)
    return resultFromTask4


def task5(spark):
    print("result of task 5")

    resultFromTask5 = spark.read.json("./sample.json")
    resultFromTask5.show(20,False)
    return resultFromTask5



def task6(resultFromTask4,resultFromTask5):
    print("result of task 6")

    mapped = resultFromTask4.select(col("*"),create_map(
               lit("LV ActivePower (kW)"),col("ActivePower"),
               lit("Wind Speed (m/s)"),col("WindSpeed"),
               lit("Theoretical_Power_Curve (KWh)"),col("Theoretical_Power_Curve"),
               lit("Wind Direction (°)"),col("WindDirection")).alias("signals")) 

    explode_data = mapped.select(col("signal_date"),col("signal_ts"),col("generation_indicator"),explode(col("signals")))

    resultFromTask6 =  explode_data.join(broadcast(resultFromTask5),explode_data["key"] == resultFromTask5["sig_name"])
    resultFromTask6.select(col("signal_date"),col("signal_ts"),col("sig_mapping_name"),col("value"),col("generation_indicator"))
    resultFromTask6.show(30,False)


if __name__ == '__main__':

    spark = session()
    read_from_delta = task1(spark)
    task2(read_from_delta)
    resultFromTask3 = task3(read_from_delta)
    resultFromTask4 = task4(resultFromTask3)
    resultFromTask5 = task5(spark)
    task6(resultFromTask4,resultFromTask5)

