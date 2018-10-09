from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql.functions import countDistinct
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt



if __name__ == '__main__':
	sc = SparkContext()
	sqlContext = SQLContext(sc)
	#read data
	df = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load('VenmoDir/venmoSample.csv')
	df2 = df
	#copy dataframe with switched user1 and user2
	df2 = df2.withColumnRenamed("user1", "user1temp").withColumnRenamed("user2", "user1").withColumnRenamed("user1temp", "user2")
	#Every relation now both directions for this df
	uniondf = df.unionAll(df2)
	df2.unpersist()
	
	#Get the number of unique users every user in the data is transacting with in any direction
	unionpairs = uniondf.groupBy("user1").agg(countDistinct("user2").alias("degree"))
	uniondf.unpersist()
	#Group that by degree for plotting dist
	degreeDist = unionpairs.groupBy("degree").agg(countDistinct("user1").alias("count"))
	
	
	pd_degreedist = degreeDist.toPandas()
	#pd_degreedist.to_csv("degreedist.csv")
	pd_degreedist[["degree","count"]] = pd_degreedist[["degree","count"]].apply(pd.to_numeric)
	
	#Take 90% of users in plot to make it appear reasonable.
	pd_degreedist = pd_degreedist.sort_values("degree")
	pd_degreedist["cumu"] = pd_degreedist["count"].cumsum()
	pd_degreedist = pd_degreedist[pd_degreedist["cumu"] < np.percentile(pd_degreedist["cumu"],90)] 
	pd_degreedist.to_csv("degreedist.csv")
	plt.gcf().clear()
	plt.bar(pd_degreedist.degree,pd_degreedist["count"])
	plt.xlabel("Degree")
	plt.ylabel("Frequency")
	plt.title("Undirected Degree Distribution for 90% of users")
	plt.savefig('Gupta_1_1.png')
	