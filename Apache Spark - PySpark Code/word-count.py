from pyspark import SparkConf, SparkContext

#Count how many times each word appears in the book
#This solution doesn;t account for capitalization or punctuation

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:////Users/abdulahad/Documents/data-engineering/SparkCourse//book.txt")
words = input.flatMap(lambda x: x.split()) #for each line in input rdd, break up by individual words, by whitespace
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
