from pyspark import SparkConf, SparkContext
import collections


#create spark context object sc using conf(with certain configurations)
#run it on local machine in on one thread
#setAppName so you can identify it in Spark UI
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram") 
sc = SparkContext(conf = conf)

#create rdd called lines
#each line from the 100k is a value in the lines rdd
lines = sc.textFile("file:////Users/abdulahad/Documents/data-engineering/sparkCourse/ml-100k/u.data")
#map will take each individual line in the rdd data and split it by white space(its a new column) then index to 
#whichever column you want 
ratings = lines.map(lambda x: x.split()[2])
#result is a normal python object, returns tuple (rating, #times of occurence)
result = ratings.countByValue()

#this is just plain python code to create an ordered dict to sort those results
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
