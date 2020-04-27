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
AllTripsDf= AllTripsDf.withColumnRenamed("_c0","medallion") .withColumnRenamed("_c1","hack_license").withColumnRenamed("_c2","vendor_id").withColumnRenamed("_c3","pickup_datetime")    .withColumnRenamed("_c4","rate_code").withColumnRenamed("_c5","store_and_fwd_flag") .withColumnRenamed("_c6","dropoff_datetime").withColumnRenamed("_c7","passenger_count").withColumnRenamed("_c8","trip_time_in_secs")    .withColumnRenamed("_c9","trip_distance").withColumnRenamed("_c10","pickup_longitude")    .withColumnRenamed("_c11","pickup_latitude").withColumnRenamed("_c12","dropoff_longitude").withColumnRenamed("_c13","dropoff_latitude").withColumnRenamed("_c14","payment_type").withColumnRenamed("_c15","fare_amount").withColumnRenamed("_c16","surcharge").withColumnRenamed("_c17","mta_tax").withColumnRenamed("_c18","tip_amount").withColumnRenamed("_c19","tolls_amount").withColumnRenamed("_c20","total_amount")

#tripsDf.createOrReplaceTempView("trips")
#faresDf.createOrReplaceTempView("fares")

#AllTrips = spark.sql("SELECT * FROM trips JOIN fares using(medallion,hack_license, vendor_id,pickup_datetime) order by trips.medallion,trips.hack_license,trips.pickup_datetime")
AllTripsDf.createOrReplaceTempView("AllTrips")

tripsRange =spark.sql("select a.passenger_count as num_of_passengers, count(*) as num_trips from  AllTrips as a where a.passenger_count >= 0 group by a.passenger_count order by a.passenger_count")
tripsRange.select(format_string('%s,%s',tripsRange.num_of_passengers,tripsRange.num_trips)).write.save('task2b-sql.out',format="text")
#print (tripsRange.show(5))
