
import pyspark
from pyspark import SparkContext
from csv import reader
import sys

from functools import reduce

sc = SparkContext()
licenses = sc.textFile(sys.argv[2],1)
fares = sc.textFile(sys.argv[1],1)
fares = fares.mapPartitions(lambda x: reader(x))
licenses = licenses.mapPartitions(lambda x: reader(x))
#removing headers from the fares and licenses data
licensesheader = licenses.first()
faresheader = fares.first()
licensesData = licenses.filter(lambda x: x != licensesheader)
faresData = fares.filter(lambda x: x != faresheader)

#joining the table on medallion  so filtering out the indices

licenseIndices = [[0], [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]]
licensesKV = licensesData.map(lambda x:(tuple(x[i] for i in licenseIndices[0]), tuple(x[i] for i in licenseIndices[1])))

fareIndices = [[0],[1,2,3,4,5,6,7,8,9,10]]
faresKV = faresData.map(lambda x:(tuple(x[i] for i in fareIndices[0]), tuple(x[i] for i in fareIndices[1])))

sortedData = faresKV.join(licensesKV).sortByKey()
#using flat map to merge the key and values of licenses and fares data

joinedData = sortedData.flatMap(lambda x:[tuple([item for item in x[0]]+[item for item in x[1][0]]+[item for item in x[1][1]])])
joinedData=joinedData.map(lambda a:reduce(lambda res,x: res+","+str(x),a))
#print (joinedData.take(1))
joinedData.saveAsTextFile("task1b.out")
