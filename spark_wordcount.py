"""Les Miserables WordCount"""

from pyspark import SparkContext

import os
import re
import sys


sc = SparkContext("local", "Les Mis Word Count") 

logFile = "/Users/vboykis/Desktop/data_lake/lesmiserables*.txt"

wordcounts = sc.textFile(logFile).map( lambda x: x.replace(',',' ').replace('.',' ').replace('-',' ').lower()) \
        .flatMap(lambda x: x.split()) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda x,y:x+y) \
        .map(lambda x:(x[1],x[0])) \
        .sortByKey(False) 

#print first 10 results 
print(wordcounts.take(10))


sc.stop()