from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from csv import reader
import sys
spark = SparkSession\
        .builder\
        .appName("assigntment_2")\
        .getOrCreate()




tripsDf = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])

faresDf = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])

tripsDf.createOrReplaceTempView("trips")
faresDf.createOrReplaceTempView("fares")

trip_fare = spark.sql("SELECT * FROM trips JOIN fares using(medallion,hack_license, vendor_id,pickup_datetime) order by trips.medallion,trips.hack_license,trips.pickup_datetime")

#print (trip_fare.show(5))
trip_fare= trip_fare.withColumn("pickup_datetime",from_unixtime(unix_timestamp("pickup_datetime"), "YYYY-MM-dd HH:mm:ss")).withColumn("dropoff_datetime",from_unixtime(unix_timestamp("dropoff_datetime"), "YYYY-MM-dd HH:mm:ss"))

trip_fare.select(format_string('%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s', trip_fare.medallion, trip_fare.hack_license, trip_fare.vendor_id, trip_fare.pickup_datetime, trip_fare.rate_code, trip_fare.store_and_fwd_flag, trip_fare.dropoff_datetime, trip_fare.passenger_count, trip_fare.trip_time_in_secs, trip_fare.trip_distance, trip_fare.pickup_longitude, trip_fare.pickup_latitude, trip_fare.dropoff_longitude, trip_fare.dropoff_latitude, trip_fare.payment_type, trip_fare.fare_amount, trip_fare.surcharge, trip_fare.mta_tax, trip_fare.tip_amount, trip_fare.tolls_amount, trip_fare.total_amount)).write.save('task1a-sql.out',format="text")
#medallionCount.select(format_string('%s,%s',medallionCount.hack_license,medallionCount.num_taxis_used)).write.save('task3a-sql.out',format="text")
