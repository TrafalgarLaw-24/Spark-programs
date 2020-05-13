from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

#we're going to find out which movie is most popular to least popular.
#open the file u.data and you'll know it's full of numbers.
#second variable in this is the corresponding number for a movie.
#we'll see movie with names in coming files in the repo.
lines = sc.textFile("Spark-programs/data/ml-100k/u.data")

#add '1' and create a tuple with every movie in the list
movies = lines.map(lambda x: (int(x.split()[1]), 1))

#now reduce by key will add all the '1' next to a movie and give a sum for every movie
movieCounts = movies.reduceByKey(lambda x, y: x + y)

#flip the tuple and sort it by key to keep the sum from lowest to highest
flipped = movieCounts.map( lambda xy: (xy[1],xy[0]) )
sortedMovies = flipped.sortByKey()

#collect the tuples as a list
results = sortedMovies.collect()

#print it and the last in the list is the most popular movie.
for result in results:
    print(result)
