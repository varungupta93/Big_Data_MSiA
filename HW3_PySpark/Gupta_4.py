import pyspark
from pyspark import SparkContext
from pyspark import HiveContext
from pyspark import SQLContext
import pandas as pd
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import unix_timestamp
#from pyspark.sql.functions import dayOfWeek
from pyspark.sql.functions import hour, date_format
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

#Function created because spark 1.6 does not support dayofweek
def daynameToNum(dayname):
	dayDict = {"Sun":1, "Mon":2, "Tue":3, "Wed":4, "Thu":5, "Fri":6, "Sat":7}
	return dayDict[dayname]

if __name__ == '__main__':
	sc = SparkContext()
	sqlContext = SQLContext(sc)
	#Read in the data
	df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('CrimeFolder/Crimes_-_2001_to_present.csv')
	#Drop Irrelevant columns
	df2 = df.select('ID', 'Date', 'Year', 'Arrest')
	df2 = df2.withColumn("Month", df2.Date.substr(0,2)) #Get Month
	#Convert 'date' to timestamp
	df2 = df2.withColumn("TimeStamp", from_unixtime(unix_timestamp(df2.Date, "MM/dd/yyyy hh:mm:ss a")))
	
	#Convert python function to spark udf
	get_daynum = udf(daynameToNum, IntegerType())
	#Day Name, Day Number, Hour of Day
	df3 = df2.withColumn("Day", date_format(df2["TimeStamp"], "E"))
	df3 = df3.withColumn("DayOfWeek", get_daynum("Day"))
	df3 = df3.withColumn("Hour", hour(df3["TimeStamp"]))
	
	#Filter for arrests
	df4 = df3.filter(col("Arrest") == True)
	
	#Results by Hour of Day
	hourdf = df4.groupBy("Hour").count()
	hourdf = hourdf.sort("Hour")
	hourdf2 = hourdf.filter(col("Hour").isNotNull())
	pd_hour = hourdf2.toPandas()
	pd_hour["Hour"] = pd_hour["Hour"] + 1
	#Plotting
	plt.gcf().clear()
	pd_hour.plot.bar(x="Hour", y="count", title="Arrests by Hour of Day", color="steelblue", legend=None)
	plt.xlabel("Hour of Day")
	plt.ylabel("Number of Arrests")
	plt.savefig('gupta_4_hour.png')
	
	#Results by Day of Week
	daydf = df4.groupBy("Day", "DayOfWeek").count()
	daydf = daydf.filter(col("Day").isNotNull())
	daydf = daydf.filter(col("DayOfWeek").isNotNull())
	daydf = daydf.sort("DayOfWeek")
	pd_day = daydf.toPandas()
	#Plotting
	plt.gcf().clear()
	pd_day.plot.bar(x="Day", y="count", title="Arrests by Day of Week", color="steelblue", legend=None)
	plt.xlabel("Day of Week")
	plt.ylabel("Number of Arrests")
	plt.savefig('gupta_4_day.png')
	
	#Results by Month of Year
	monthdf = df4.groupBy("Month").count()
	monthdf = monthdf.filter(col("Month").isNotNull())
	monthdf = monthdf.sort("Month")
	pd_month = monthdf.toPandas()
	#Plotting
	plt.gcf().clear()
	pd_month.plot.bar(x="Month", y="count", title="Arrests by Month of Year", color="steelblue", legend=None)
	plt.xlabel("Month")
	plt.ylabel("Number of Arrests")
	plt.savefig('gupta_4_month.png')
	