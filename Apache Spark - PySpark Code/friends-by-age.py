from pyspark import SparkConf, SparkContext
import collections

#Deals with Key/Value rdd's and average friends by age

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("file:////Users/abdulahad/Documents/data-engineering/sparkCourse/fakefriends.csv")
rdd = lines.map(parseLine)
#mapValues --> we add a 1 so we can later add up times up occurence  (age, (#friends,1)) 
#reduceByKey --> groupby age group, add up #friends for that age group and times of occurence 
#now we have for each age group --> (age, (total #friends,total occurences)) 
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
#now divide total #friends/total occurences, we get (age, avg # of friends for that age) 
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print(result)


#for sorted results do this
sortedResults = collections.OrderedDict(sorted(results))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))