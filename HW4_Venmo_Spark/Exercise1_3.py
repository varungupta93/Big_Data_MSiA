from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql.functions import countDistinct, col, lit, min, concat
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import datetime
from dateutil.relativedelta import relativedelta

if __name__ == '__main__':
	sc = SparkContext()
	sqlContext = SQLContext(sc)
	df = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load('VenmoDir/venmoSample.csv')
	df2 = df.select([df.user1.cast("string"), df.user2.cast("string"), df["datetime"]])
	df.unpersist()
	#combine users to get transaction
	df_u1u2 = df2.withColumn("u1u2", concat(col("user1"), lit("_") , col("user2")))
	df_u2u1 = df2.withColumn("u1u2", concat(col("user2"), lit("_"), col("user1")))               

	#Get first date the transaction occurred on (really only useful for second part with bucketing, otherwise just used to make transactions unique)
	df_u1u2_uniq = df_u1u2.select(["u1u2", "datetime"]).groupBy("u1u2").agg(min("datetime")).alias('datetime1')
	df_u2u1_uniq = df_u2u1.select(["u1u2", "datetime"]).groupBy("u1u2").agg(min("datetime")).alias('datetime2')

	df_joined = df_u1u2_uniq.join(df_u2u1_uniq, ["u1u2"])
	df_allrels = df_u1u2_uniq.unionAll(df_u2u1_uniq)
	recip_relations = df_joined.count()/2
	total_relations = df_allrels.select(["u1u2"]).distinct().count()/2
	prop_recip = recip_relations/total_relations
	
	with open("Exercise1_3.txt", "w") as output:
		output.write("The overall proportion of reciprocal relationships is " + str(round(prop_recip * 100,2)) + "%")


	#####Plotting this rate over time############
	strtDate = datetime.datetime(2010,1,1,0,0,0)
	endDate = datetime.datetime(2016,6,1,0,0,0) #Means last bucket is only 5 months
	
	#Get all bucket end dates
	alldates = [strtDate + relativedelta(months=6 * i) for i in range(1,13)]
	alldates.append(endDate) #Because according to the question, last bucket is 5 months.

	listdicts = []
	for bucketend in alldates:
	    df_u1u2_filt = df_u1u2.filter((col("datetime") > strtDate) & (col("datetime") < bucketend))
	    df_u2u1_filt = df_u2u1.filter((col("datetime") > strtDate) & (col("datetime") < bucketend))
	    df_u1u2_uniq = df_u1u2_filt.select(["u1u2", "datetime"]).groupBy("u1u2").agg(min("datetime")).alias('datetime1')
	    df_u2u1_uniq = df_u2u1_filt.select(["u1u2", "datetime"]).groupBy("u1u2").agg(min("datetime")).alias('datetime2')
	    df_joined = df_u1u2_uniq.join(df_u2u1_uniq, ["u1u2"])
	    df_allrels = df_u1u2_uniq.unionAll(df_u2u1_uniq)
	    recip_relations = df_joined.count()/2
	    total_relations = df_allrels.select(["u1u2"]).distinct().count()/2
		# df_u1u2_filt.unpersist()
		# df_u2u1_filt.unpersist()
		# df_u1u2_uniq.unpersist()
		# df_u2u1_uniq.unpersist()
	    if total_relations > 0:
		    prop_recip = 100 * recip_relations/total_relations
	    else:
		    prop_recip = 0
	    bucket_dict = {"End_Date":bucketend.strftime("%m/%d/%Y"), "Recip_Prop": prop_recip}
	    listdicts.append(bucket_dict)

	#Create pandas df for plotting
	pd_df = pd.DataFrame(listdicts)
	plt.gcf().clear()
	positions = np.arange(len(pd_df.End_Date))
	plt.bar(positions, pd_df.Recip_Prop, align='center', alpha=0.5)
	plt.xticks(positions, pd_df.End_Date, rotation='vertical')
	#plt.bar(pd_df.End_Date,pd_df.Recip_Prop)
	plt.xlabel("End-Date of Bucket")
	plt.ylabel("Percentage of Relationships that are Reciprocal")
	plt.title("Reciprocal Relationship Rate using transactions from 01/01/2010 to 06/01/2016")
	plt.subplots_adjust(bottom=0.5)
	plt.savefig('Exercise1_3.png')
	



