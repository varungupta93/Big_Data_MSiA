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
	df = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load('VenmoDir/venmoSample.csv')
	df2 = df
	
	#separate indegree df and outdegree df
	outdeg = df2.groupBy("user1").agg(countDistinct("user2").alias("outdegree"))
	indeg = df2.groupBy("user2").agg(countDistinct("user1").alias("indegree"))
	
	#Count users with value of each degree type for plotting
	outdegreeDist = outdeg.groupBy("outdegree").agg(countDistinct("user1").alias("usercount"))
	indegreeDist = indeg.groupBy("indegree").agg(countDistinct("user2").alias("usercount"))
	
	#Pandas for plotting
	pd_outdegreedist = outdegreeDist.toPandas()
	pd_indegreedist = indegreeDist.toPandas()
	#pd_degreedist.to_csv("degreedist.csv")
	pd_outdegreedist[["outdegree","usercount"]] = pd_outdegreedist[["outdegree","usercount"]].apply(pd.to_numeric)
	pd_indegreedist[["indegree","usercount"]] = pd_indegreedist[["indegree","usercount"]].apply(pd.to_numeric)
	pd_outdegreedist = pd_outdegreedist.sort_values("outdegree")
	pd_indegreedist = pd_indegreedist.sort_values("indegree")
	
	pd_outdegreedist["cumu"] = pd_outdegreedist["usercount"].cumsum()
	pd_indegreedist["cumu"] = pd_indegreedist["usercount"].cumsum()
	#Get first 90% based on degree
	pd_outdegreedist = pd_outdegreedist[pd_outdegreedist["cumu"] < np.percentile(pd_outdegreedist["cumu"],90)]
	pd_indegreedist = pd_indegreedist[pd_indegreedist["cumu"] < np.percentile(pd_indegreedist["cumu"],90)]
	
	
	#Plot the two distributions
	plt.gcf().clear()
	plt.bar(pd_outdegreedist.outdegree,pd_outdegreedist["usercount"])
	plt.xlabel("Out-Degree")
	plt.ylabel("Frequency")
	plt.title("Out-Degree Distribution")
	plt.savefig('Exercise1_2a.png')
	
	plt.gcf().clear()
	plt.bar(pd_indegreedist.indegree,pd_indegreedist["usercount"])
	plt.xlabel("In-Degree")
	plt.ylabel("Frequency")
	plt.title("In-Degree Distribution")
	plt.savefig('Exercise1_2b.png')
	