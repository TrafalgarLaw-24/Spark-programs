from pyspark import SparkConf, SparkContext
import collections

#we're going to find out how many movies where present in the ratings 1 to 5
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

#change the path below to your working directory.
lines = sc.textFile("Spark-programs/data/ml-100k/u.data")
#taking the rating alone
ratings = lines.map(lambda x: x.split()[2])
#countByValue will give us the sum of the values in dictionary format
result = ratings.countByValue()

#sorting it and printing
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
