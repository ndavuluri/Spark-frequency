from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
#spark execution context
    sc = SparkContext(appName="PythonWordCount2")
#Reading text from all the 10 files in the given input directory 
    lines = sc.textFile(sys.argv[1]+"/*.txt", 1)
  #Get double words  
    def pairs(param):
	templist = param.split(" ")

       	index = 0 
	start = 0
	dup = []
	for words in templist:
		if(start == 0):
			start = 1
			index+=1
		else:
			dup.append((str(templist[index-1]),str(words)))
			index+=1
	return dup
#get count of the double words 
    counts = lines.flatMap(pairs).map(lambda x: (str(x),1)).reduceByKey(add)
    counts.saveAsTextFile(sys.argv[2])

    sc.stop()
