
import pyspark
from pyspark import SparkContext
from csv import reader
import sys
from functools import reduce
sc = SparkContext()
trips = sc.textFile(sys.argv[1],1)
fares = sc.textFile(sys.argv[2],1)
trips = trips.mapPartitions(lambda x: reader(x))
fares = fares.mapPartitions(lambda x: reader(x))
#removing headers from the fares and trips data
tripsheader = trips.first()
faresheader = fares.first()
tripsData = trips.filter(lambda x: x != tripsheader)
faresData = fares.filter(lambda x: x != faresheader)

#joining the table on medallion hack license vendor id and pickup_datetime so filtering out the fareIndices

tripIndices = [[0,1,2,5], [3,4,6,7,8,9,10,11,12,13]]
tripsKV = tripsData.map(lambda x:(tuple(x[i] for i in tripIndices[0]), tuple(x[i] for i in tripIndices[1])))

fareIndices = [[0,1,2,3],[4,5,6,7,8,9,10]]
faresKV = faresData.map(lambda x:(tuple(x[i] for i in fareIndices[0]), tuple(x[i] for i in fareIndices[1])))

result = tripsKV.join(faresKV)
#using flat map to merge the key and values of trips and fares data
result = result.flatMap(lambda x:[tuple([item for item in x[0]]+[item for item in x[1][0]]+[item for item in x[1][1]])])
result=(result.sortBy(keyfunc=lambda x: (x[0],x[1],x[3])))
result=result.map(lambda a:reduce(lambda res,x: res+","+str(x),a))
#print (result.take(1))
result.saveAsTextFile("task1a.out")
