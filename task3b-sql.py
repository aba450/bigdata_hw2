

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




AllTripsDf = spark.read.format('csv').options(header='false',inferschema='true').load(sys.argv[1])

#faresDf = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])
#faresDf= faresDf.withColumn("pickup_datetime",date_format("pickup_datetime", "YYYY-MM-dd"))
#tripsDf.createOrReplaceTempView("trips")
#faresDf.createOrReplaceTempView("fares")
AllTripsDf= AllTripsDf.withColumnRenamed("_c0","medallion") .withColumnRenamed("_c1","hack_license").withColumnRenamed("_c2","vendor_id").withColumnRenamed("_c3","pickup_datetime")    .withColumnRenamed("_c4","rate_code").withColumnRenamed("_c5","store_and_fwd_flag") .withColumnRenamed("_c6","dropoff_datetime").withColumnRenamed("_c7","passenger_count").withColumnRenamed("_c8","trip_time_in_secs")    .withColumnRenamed("_c9","trip_distance").withColumnRenamed("_c10","pickup_longitude")    .withColumnRenamed("_c11","pickup_latitude").withColumnRenamed("_c12","dropoff_longitude").withColumnRenamed("_c13","dropoff_latitude").withColumnRenamed("_c14","payment_type").withColumnRenamed("_c15","fare_amount").withColumnRenamed("_c16","surcharge").withColumnRenamed("_c17","mta_tax").withColumnRenamed("_c18","tip_amount").withColumnRenamed("_c19","tolls_amount").withColumnRenamed("_c20","total_amount")

#AllTrips = spark.sql("SELECT * FROM trips JOIN fares using(medallion,hack_license, vendor_id,pickup_datetime) order by trips.medallion,trips.hack_license,trips.pickup_datetime")
#AllTripsDf= AllTripsDf.withColumn("pickup_datetime",date_format("pickup_datetime", "YYYY-MM-dd"))
#AllTripsDf.createOrReplaceTempView("AllTrips")
AllTripsDf.createOrReplaceTempView("AllTrips")

same_time =spark.sql("select a.medallion as medallion,a.pickup_datetime as pickup_datetime from AllTrips as a group by a.medallion, a.pickup_datetime having count(*) >1 order by a.medallion,a.pickup_datetime")
same_time= same_time.withColumn("pickup_datetime",from_unixtime(unix_timestamp("pickup_datetime"), "YYYY-MM-dd HH:mm:ss"))


#print (same_time.show(5))
same_time.select(format_string('%s,%s',same_time.medallion,same_time.pickup_datetime)).write.save('task3b-sql.out',format="text")
