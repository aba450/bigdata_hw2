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




LicenseFaredDf = spark.read.format('csv').options(header='false',inferschema='true').load(sys.argv[1])

#faresDf = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])
#faresDf= faresDf.withColumn("pickup_datetime",date_format("pickup_datetime", "YYYY-MM-dd"))
#licensesDf = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[3])

#LicenseFaredDf= LicenseFaredDf.withColumnRenamed("_c0","medallion") .withColumnRenamed("_c1","hack_license").withColumnRenamed("_c2","vendor_id").withColumnRenamed("_c3","pickup_datetime") .withColumnRenamed("_c4","fare_amount").withColumnRenamed("_c5","surcharge").withColumnRenamed("_c6","mta_tax").withColumnRenamed("_c7","tip_amount").withColumnRenamed("_c8","tolls_amount").withColumnRenamed("_c9","total_amount").withColumnRenamed("_c10","name") .withColumnRenamed("_c11","type").withColumnRenamed("_c12","current_status").withColumnRenamed("_c13","DMV_license_plate") .withColumnRenamed("_c14","vehicle_VIN_number").withColumnRenamed("_c15","vehicle_type").withColumnRenamed("_c16","model_year").withColumnRenamed("_c17","medallion_type").withColumnRenamed("_c18","agent_number").withColumnRenamed("_c19","agent_name").withColumnRenamed("_c20","agent_telephone_number").withColumnRenamed("_c21","agent_website").withColumnRenamed("_c22","agent_address").withColumnRenamed("_c23","last_updated_date").withColumnRenamed("_c24","last_updated_time")

LicenseFaredDf= LicenseFaredDf.withColumnRenamed("_c0","medallion") .withColumnRenamed("_c1","hack_license").withColumnRenamed("_c2","vendor_id").withColumnRenamed("_c3","pickup_datetime") .withColumnRenamed("_c4","payment_type").withColumnRenamed("_c5","fare_amount").withColumnRenamed("_c6","surcharge").withColumnRenamed("_c7","mta_tax").withColumnRenamed("_c8","tip_amount").withColumnRenamed("_c9","tolls_amount").withColumnRenamed("_c10","total_amount") .withColumnRenamed("_c11","name").withColumnRenamed("_c12","type").withColumnRenamed("_c13","current_status").withColumnRenamed("_c14","DMV_license_plate").withColumnRenamed("_c15","vehicle_VIN_number").withColumnRenamed("_c16","vehicle_type").withColumnRenamed("_c17","model_year").withColumnRenamed("_c18","medallion_type").withColumnRenamed("_c19","agent_number").withColumnRenamed("_c20","agent_name").withColumnRenamed("_c21","agent_telephone_number").withColumnRenamed("_c22","agent_website").withColumnRenamed("_c23","agent_address").withColumnRenamed("_c24","last_updated_date").withColumnRenamed("_c25","last_updated_time")


#tripsDf.createOrReplaceTempView("trips")
#faresDf.createOrReplaceTempView("fares")
LicenseFaredDf.createOrReplaceTempView("licenseFares")

#AllTrips = spark.sql("SELECT * FROM trips JOIN fares using(medallion,hack_license, vendor_id,pickup_datetime) order by trips.medallion,trips.hack_license,trips.pickup_datetime")
#AllTrips= AllTrips.withColumn("pickup_datetime",date_format("pickup_datetime", "YYYY-MM-dd"))
#AllTrips.createOrReplaceTempView("AllTrips")

#joinedDf = spark.sql("SELECT * FROM licenses JOIN AllTrips using(medallion) order by licenses.medallion")
#joinedDf.createOrReplaceTempView("licensestrips")

tipPercentage = spark.sql("select a.vehicle_type as vehicle_type,count(*) as total_trips,sum(a.fare_amount) as total_revenue,((100/count(*))*(sum(a.tip_amount/a.fare_amount))) as average_tip_percentage  from licenseFares as a group by a.vehicle_type order by a.vehicle_type")

#print (tipPercentage.show(5))
tipPercentage.select(format_string('%s,%s,%s,%s',tipPercentage.vehicle_type,tipPercentage.total_trips,tipPercentage.total_revenue,tipPercentage.average_tip_percentage)).write.save('task4a-sql.out',format="text")
