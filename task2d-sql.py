from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import string
from pyspark.conf import SparkConf
from csv import reader
conf = SparkConf()

spark = SparkSession\
        .builder\
        .appName("big_data")\
        .getOrCreate()

#picking the output file from hfs and then adding headers to run spark sql
AllTrips = spark.read.format('csv').options(header='false',inferschema='true').load(sys.argv[1])
AllTrips=AllTrips.withColumnRenamed("_c0","medallion").withColumnRenamed("_c1","hack_license").withColumnRenamed("_c2","vendor_id").withColumnRenamed("_c3","pickup_datetime").withColumnRenamed("_c4","rate_code").withColumnRenamed("_c5","store_and_fwd_flag").withColumnRenamed("_c6","dropoff_datetime").withColumnRenamed("_c7","passenger_count").withColumnRenamed("_c8","trip_time_in_secs").withColumnRenamed("_c9","trip_distance").withColumnRenamed("_c10","pickup_longitude").withColumnRenamed("_c11","pickup_latitude").withColumnRenamed("_c12","dropoff_longitude").withColumnRenamed("_c13","dropoff_latitude").withColumnRenamed("_c14","payment_type").withColumnRenamed("_c15","fare_amount").withColumnRenamed("_c16","surcharge").withColumnRenamed("_c17","mta_tax").withColumnRenamed("_c18","tip_amount").withColumnRenamed("_c19","tolls_amount").withColumnRenamed("_c20","total_amount")

AllTrips.createOrReplaceTempView("AllTrips")


average_days_driven = spark.sql("select a.medallion as medallion,count(*) as total_trips,count(distinct date(a.pickup_datetime)) as days_driven, round((count(*)/count(distinct date(a.pickup_datetime))),2) as average from AllTrips as a group by a.medallion order by a.medallion")


average_days_driven.select(format_string('%s, %s, %s, %s', average_days_driven.medallion, average_days_driven.total_trips, average_days_driven.days_driven, average_days_driven.average)).write.save('task2d-sql.out',format="text")
