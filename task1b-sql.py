
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import string
from pyspark.sql import SQLContext
import sys
from csv import reader

spark = SparkSession\
        .builder\
        .appName("assignment_2")\
        .getOrCreate()

#reading the file into data frame
licensesDf = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])

faresDf = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])

#creating view to manipulate data using spark sql
licensesDf = licensesDf.withColumn('name', regexp_replace('name', ',', ' '))
licensesDf.createOrReplaceTempView("licenses")
faresDf.createOrReplaceTempView("fares")
#licenses = licenses.withColumn('name', regexp_replace('name', ',', ' '))
joinedDf = spark.sql("SELECT * FROM fares JOIN licenses using(medallion) order by fares.medallion, fares.hack_license, fares.pickup_datetime")
joinedDf= joinedDf.withColumn("pickup_datetime",from_unixtime(unix_timestamp("pickup_datetime"), "YYYY-MM-dd HH:mm:ss"))

#print (joinedDf.show(1))

joinedDf.select(format_string('%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s', joinedDf.medallion, joinedDf.hack_license, joinedDf.vendor_id, joinedDf.pickup_datetime, joinedDf.payment_type, joinedDf.fare_amount, joinedDf.surcharge, joinedDf.mta_tax, joinedDf.tip_amount, joinedDf.tolls_amount, joinedDf.total_amount,joinedDf.name, joinedDf.type, joinedDf.current_status, joinedDf.DMV_license_plate, joinedDf.vehicle_VIN_number, joinedDf.vehicle_type, joinedDf.model_year, joinedDf.medallion_type, joinedDf.agent_number, joinedDf.agent_name, joinedDf.agent_telephone_number, joinedDf.agent_website, joinedDf.agent_address, joinedDf.last_updated_date, joinedDf.last_updated_time)).write.save('task1b-sql.out',format="text")

#joinedDf.save(?~@~\task1b-sql.out?~@~],format=?~@~]text?~@~])
