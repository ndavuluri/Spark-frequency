from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
#spark execution context
    sc = SparkContext(appName="PythonWordCount")
#Reading text from all the 10 files in the given input directory 
    lines = sc.textFile(sys.argv[1]+"/*.txt", 1)
    words = lines.flatMap(lambda x: x.split(' '))  
    counts = words.map(lambda x: (str(x), 1)) \
                  .reduceByKey(add)
    output = counts.collect()
#Save to file
    counts.saveAsTextFile(sys.argv[2])

    sc.stop()
