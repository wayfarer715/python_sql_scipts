import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    #regular expressions r'\W+' breaks up by words autmatically ignoring punctuation
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:////Users/abdulahad/Documents/data-engineering/SparkCourse/book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
