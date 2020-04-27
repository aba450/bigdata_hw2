

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



LicenseFaredDf= LicenseFaredDf.withColumnRenamed("_c0","medallion") .withColumnRenamed("_c1","hack_license").withColumnRenamed("_c2","vendor_id").withColumnRenamed("_c3","pickup_datetime") .withColumnRenamed("_c4","payment_type").withColumnRenamed("_c5","fare_amount").withColumnRenamed("_c6","surcharge").withColumnRenamed("_c7","mta_tax").withColumnRenamed("_c8","tip_amount").withColumnRenamed("_c9","tolls_amount").withColumnRenamed("_c10","total_amount").withColumnRenamed("_c11","name") .withColumnRenamed("_c12","type").withColumnRenamed("_c13","current_status").withColumnRenamed("_c14","DMV_license_plate").withColumnRenamed("_c15","vehicle_VIN_number").withColumnRenamed("_c16","vehicle_type").withColumnRenamed("_c17","model_year").withColumnRenamed("_c18","medallion_type").withColumnRenamed("_c19","agent_number").withColumnRenamed("_c20","agent_name").withColumnRenamed("_c21","agent_telephone_number").withColumnRenamed("_c22","agent_website").withColumnRenamed("_c23","agent_address").withColumnRenamed("_c24","last_updated_date").withColumnRenamed("_c25","last_updated_time")




#tripsDf.createOrReplaceTempView("trips")
#faresDf.createOrReplaceTempView("fares")
LicenseFaredDf.createOrReplaceTempView("licenseFares")

tipPercentage = spark.sql("select a.agent_name as agent_name,sum(a.fare_amount) as total_revenue from licenseFares as a group by a.agent_name order by total_revenue desc, a.agent_name limit 10")

#print (tipPercentage.show(5))
tipPercentage.select(format_string('%s,%s',tipPercentage.agent_name,tipPercentage.total_revenue)).write.save('task4c-sql.out',format="text")
