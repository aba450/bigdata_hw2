import pyspark
from pyspark import SparkContext
from csv import reader
import sys


sc = SparkContext()
AllTrips = sc.textFile(sys.argv[1])
#fares = sc.textFile(sys.argv[2],1)
AllTrips = AllTrips.map(lambda line:line.split(','))
#print (AllTrips.take(1))

result = AllTrips.map(lambda x: ('invalid_fare_amount',1) if float(x[15])<0 else ('invalid_fare_amount',0))


#licenses_map = licenses_rdd.map(lambda x: (','.join(x[0:1]) ,','.join(x[1:])))

#joining fares to licenses
#license_fares = fares_map.join(licenses_map)
result = result.reduceByKey(lambda x,y:x+y).sortByKey()

#result = result.filter(lambda x: x[0] =='invalid_fare_amount')
#print (result.take(5))
result.saveAsTextFile("task3a.out")
