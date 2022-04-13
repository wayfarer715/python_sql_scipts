from pyspark import SparkConf, SparkContext
import collections

#find total amount spent by each customer
#then sort by total spent ascending

conf = SparkConf().setMaster("local").setAppName("CostByCustomer")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    itemCost = float(fields[2])
    return (customerID, itemCost)

lines = sc.textFile("file:////Users/abdulahad/Documents/data-engineering/sparkCourse/customer-orders.csv")
rdd = lines.map(parseLine)
totalPerCustomer = rdd.reduceByKey(lambda x, y: x+y)
#flip the tuple to make itemCost the new key, then sort by key
totalPerCustomerSorted = totalPerCustomer.map(lambda x: (x[1], x[0])).sortByKey()

results = totalPerCustomerSorted.collect()
for result in results:
    print(result)