from pyspark import SparkConf, SparkContext

#we're going to find out how many words are there in any book/text file using Spark

#set master to local when you're running in your machine
conf = SparkConf().setMaster("local").setAppName("WordCount")
#initialize SparkContext as it is like the entry point to spark core
sc = SparkContext(conf = conf)

#reading the book.txt file into an RDD
input = sc.textFile("C:/Users/siraj/github/Spark-programs/data/book.txt")
#flatmap will transform a corpus of texts into words. we're spliting it word by word
words = input.flatMap(lambda x: x.split())
#countByValue will return the sum of each word present in the object.
wordCounts = words.countByValue()

#iterate through the object to get the word and its corresponding number of occurence.
for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
