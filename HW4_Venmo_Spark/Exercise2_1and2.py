from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql.functions import countDistinct, col, lit, min, concat, date_format, to_date
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import datetime
from dateutil.relativedelta import relativedelta
import csv
import re


def getEmojis(desc, emoji_regex):
    emojis = re.findall(emoji_regex, desc)
    return emojis

if __name__ == '__main__':
	sc = SparkContext()
	sc.setLogLevel("ERROR")
	sqlContext = SQLContext(sc)
	df = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load('VenmoDir/venmoSample.csv')
	myRDDnew = df.withColumn('Weekday', date_format('datetime', 'E')).rdd
	
	myRDDnew2 = myRDDnew.map(lambda x: list(x))
	myRDD3 = myRDDnew2.map(lambda x: (x[7],x[4]))
	
	emoji_regex = r'[^\w\s,.\'\";:@#\\$%&!)(-/\x00\uFE0F]'
	myRDD4 = myRDD3.map(lambda x: (x[0],getEmojis(x[1], emoji_regex)))

	#Overall
	overall = myRDD4.flatMap(lambda x: x[1]).map(lambda x: (x,1))
	emojicounts = overall.reduceByKey(lambda x, y : x + y)
	topemojis = emojicounts.top(10, key= lambda x: x[1])


	#By day
	myRDD5 = myRDD4.flatMap(lambda x: [(x[0], em) for em in x[1]])
	Flattened = myRDD5.map(lambda x: (x,1))
	byDay = Flattened.reduceByKey(lambda x,y : x + y)
	#rearrange
	dayKey = byDay.map(lambda x: (x[0][0], [(x[0][1],x[1])]))
	
	#Create list of (emoji,count) tuples
	groupedDays = dayKey.reduceByKey(lambda x,y: x + y)
	#Sort the emojis by counts
	groupedDays = groupedDays.map(lambda x: (x[0],sorted(x[1], key= lambda y: y[1], reverse = True)))
	#Take top 5
	top5byday = groupedDays.map(lambda x: (x[0], x[1][0:5])).collect()
	
	with open("Exercise2_1and2.txt", "w") as output:
		output.write("Overall  Top 10:\nEmoji, Count")
		for tup in topemojis:
			output.write(",".join(str(s) for s in tup) + "\n")
		output.write("\n\nTop Emojis by day")
		output.write("Weekday - (Emoji, Count) list \n")
		
		for t in top5byday:
			output.write(t[0] + "-")
			output.write(','.join(str(s) for s in t[1]) + '\n')