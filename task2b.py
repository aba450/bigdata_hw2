
import pyspark
from pyspark import SparkContext
from csv import reader
import sys

sc = SparkContext()
AllTrips = sc.textFile(sys.argv[1])
#fares = sc.textFile(sys.argv[2],1)
AllTrips = AllTrips.map(lambda line:line.split(','))
#fares = fares.mapPartitions(lambda x: reader(x))


# mapping using lambda the range for fare amount from the Alltrips data
result = AllTrips.map(lambda x: (x[7],1))

result = result.reduceByKey(lambda x,y:x+y).sortByKey()
#print (result.take(6))
result.saveAsTextFile("task2b.out")
