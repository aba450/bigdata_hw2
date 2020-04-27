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
amount_range = AllTrips.map(lambda x: ('[00, 05]',1) if 0<=float(x[15])<=5 else

                            (('[05, 15]',1) if 5<float(x[15])<=15 else
                            (('[15, 30]',1) if 15<float(x[15])<=30 else
                            (('[30, 50]',1) if 10<float(x[15])<=50 else
                            (('[50, 100]',1) if 50<float(x[15])<=100 else
                             ('[>100]',1))))))

amount_range = amount_range.reduceByKey(lambda x,y:x+y).sortByKey()
#print (amount_range.take(6))
amount_range.saveAsTextFile("task2a.out")
