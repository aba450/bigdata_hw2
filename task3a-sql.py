

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import string
from pyspark.sql import SQLContext
from csv import reader
spark = SparkSession\
        .builder\
        .appName("assigntment_2")\
        .getOrCreate()




#tripsDf = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])

AllTripsDf = spark.read.format('csv').options(header='false',inferschema='true').load(sys.argv[1])
#faresDf= faresDf.withColumn("pickup_datetime",date_format("pickup_datetime", "YYYY-MM-dd"))
#AllTripsDf.createOrReplaceTempView("AllTrips")
#faresDf.createOrReplaceTempView("fares")
AllTripsDf= AllTripsDf.withColumnRenamed("_c0","medallion") .withColumnRenamed("_c1","hack_license").withColumnRenamed("_c2","vendor_id").withColumnRenamed("_c3","pickup_datetime")    .withColumnRenamed("_c4","rate_code").withColumnRenamed("_c5","store_and_fwd_flag") .withColumnRenamed("_c6","dropoff_datetime").withColumnRenamed("_c7","passenger_count").withColumnRenamed("_c8","trip_time_in_secs")    .withColumnRenamed("_c9","trip_distance").withColumnRenamed("_c10","pickup_longitude")    .withColumnRenamed("_c11","pickup_latitude").withColumnRenamed("_c12","dropoff_longitude").withColumnRenamed("_c13","dropoff_latitude").withColumnRenamed("_c14","payment_type").withColumnRenamed("_c15","fare_amount").withColumnRenamed("_c16","surcharge").withColumnRenamed("_c17","mta_tax").withColumnRenamed("_c18","tip_amount").withColumnRenamed("_c19","tolls_amount").withColumnRenamed("_c20","total_amount")

#AllTrips = spark.sql("SELECT * FROM trips JOIN fares using(medallion,hack_license, vendor_id,pickup_datetime) order by trips.medallion,trips.hack_license,trips.pickup_datetime")
#AllTrips= AllTrips.withColumn("pickup_datetime",date_format("pickup_datetime", "YYYY-MM-dd"))
AllTripsDf.createOrReplaceTempView("AllTrips")
invalid_fare =spark.sql("select count(*) as num_invalid_fare from AllTrips as a where a.fare_amount <0")
#invalid_fare =spark.sql("select count(*) as num_invalid_fare from AllTrips as a where a.fare_amount <0")
#print (invalid_fare.show(1))
invalid_fare.select(format_string('%s',invalid_fare.num_invalid_fare)).write.save('task3a-sql.out',format="text")
