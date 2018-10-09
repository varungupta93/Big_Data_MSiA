from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import col
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


if __name__ == '__main__':
	sc = SparkContext()
	sqlContext = SQLContext(sc)
	df = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load('CrimeFolder/Crimes_-_2001_to_present.csv')
	df = df.select("ID", 'Date','Year')
	#Get month column and group data by month
	df = df.withColumn("Month", df.Date.substr(0,2))
	groupedbymonth = df.groupby('Month').count()
	#Number of years the month appears in in the data, for taking mean
	groupedwithyears = df.groupby('Month').agg(countDistinct("Year"))
	finaldf = groupedbymonth.join(groupedwithyears, "Month")
	
	#get average per month
	finaldf2 = finaldf.withColumn("Avg", col("count") / col("count(Year)"))
	finaldf2 = finaldf2.sort("Month", ascending = True)
	pd_df = finaldf2.toPandas()
	plt.gcf().clear()
	plt.bar(pd_df.Month,pd_df.Avg)
	plt.xlabel("Month")
	plt.ylabel("Average # of Crimes")
	plt.title("Average Number of Crimes per Month")
	plt.savefig('Gupta_1.png')
	plt.show()
