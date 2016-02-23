from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 3:#Checking for sufficient arguments
        print("Usage: wordcount <file>", file=sys.stderr) 
        exit(-1)
#spark execution context
    sc = SparkContext(appName="PythonWordCount3")
#Reading text from all the 10 files in the given input directory 
    lines = sc.textFile(sys.argv[1]+"/*.txt", 1)
#Text that has the words to be matched 
    text = "but peter took him up saying stand up i myself also am a man and as he talked with him he went in and found many that were come together and he said unto them ye know how that it is an    unlawful thing for a man that is a jew to keep company or come unto one of another nation but god hath shewed me that i should not call any man common or unclean therefore came i unto you without gainsaying as soon as i was sent for i ask therefore for what intent ye have sent for me and cornelius said four days ago i was fasting until this hour and at the ninth hour i prayed in my house and behold a man stood before me in bright clothing"
#splitting into word
    searchwords = text.split()
    dummy = sc.parallelize(searchwords)
#Get words
    def findword(param):
	templist = param.split(" ")
	return templist
# Get count for words in the data
    counts = lines.flatMap(findword).map(lambda x: (str(x),1)).reduceByKey(add)
#get count for search words
    counts2 = dummy.map(lambda x: (str(x),0)).reduceByKey(add)
#join both and get the count
    temp = counts.join(counts2).map(lambda x:(x[0],x[1][0]))
#    counts.collect
    temp.saveAsTextFile(sys.argv[2])


    sc.stop()
