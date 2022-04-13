from pyspark import SparkConf, SparkContext


#Filtering on RDD example

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0 #convert to Farenheit
    return (stationID, entryType, temperature)

lines = sc.textFile("file:////Users/abdulahad/Documents/data-engineering/SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)
#filter takes function that returns boolean, if true then its passed to your new rdd, otherwise discarded
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
#notice use map for both key and value, transform (stationID, entryType, temperature) to (stationID, temperature)
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
#aggregate by stationID and get min temp for each stationID
#getting min of each station throughout different days in year
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
