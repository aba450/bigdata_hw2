
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

tripsRange =spark.sql("select case when fare_amount>= 0 and fare_amount<=5 then 'a' when fare_amount>5 and fare_amount<= 15 then 'b' when fare_amount>15 and fare_amount<=30 then 'c' when fare_amount >30 and fare_amount<=50 then 'd' when fare_amount >50 and fare_amount <=100 then 'e' else 'OTHERS' end as `Range` from AllTrips")
#tripsRange =spark.sql("select case when c.fare_amount>= 0 and c.fare_amount<=5 then 'a' when c.fare_amount>5 and c.fare_amount<= 15 then 'b' when c.fare_amount>15 and c.fare_amount<=30 then 'c' when c.fare_amount >30 and c.fare_amount<=50 then 'd' when c.fare_amount >50 and c.fare_amount <=100 then 'e' else 'OTHERS' end as `Range` from AllTrips")

tripsRange.createOrReplaceTempView("tripsRange")
tripsRangeDf =spark.sql("select Range as amount_range,count(*) as num_trips  from tripsRange group by `Range`order by amount_range")
tripsRangeDf.select(format_string('%s,%s',tripsRangeDf.amount_range,tripsRangeDf.num_trips)).write.save('task2a-sql.out',format="text")
