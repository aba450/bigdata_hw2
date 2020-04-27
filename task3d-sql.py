





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
AllTripsDf.createOrReplaceTempView("AllTrips")

medallionCount = spark.sql("select a.hack_license as hack_license,count(distinct(a.medallion)) as num_taxis_used from AllTrips as a group by a.hack_license order by a.hack_license")
#tripsPercentage.createOrReplaceTempView("tripsPercentage")

#percentage_trips = spark.sql("select a.medallion as medallion, round((a.trips *100/a.totaltrips),2) as percentage_of_trips from tripsPercentage as a where a.trips>0 group by a.medallion,a.trips,a.totaltrips")

#print (medallionCount.show(5))
medallionCount.select(format_string('%s,%s',medallionCount.hack_license,medallionCount.num_taxis_used)).write.save('task3d-sql.out',format="text")
