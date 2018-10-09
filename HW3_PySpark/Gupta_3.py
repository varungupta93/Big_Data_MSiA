import pyspark
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark import HiveContext
from pyspark.ml import Pipeline
from pyspark.sql.functions import countDistinct, round
from pyspark.sql.functions import concat
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import weekofyear
from pyspark.sql.functions import month
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.mllib.tree import RandomForestModel, RandomForest
from pyspark.sql.functions import lag
from pyspark.mllib.linalg import Vectors
import pandas as pd
import datetime


'''Function to read comma-separated text accurately'''
def readerfn(x):
    csv_reader = csv.reader([x])
    rowfields = None
    for row in csv_reader:
        rowfields = row
    return rowfields


	
	

if __name__ == '__main__':
	sc = SparkContext()
	sqlContext = SQLContext(sc)
	hiveContext = HiveContext(sc)
	df = hiveContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('CrimeFolder/Crimes_-_2001_to_present.csv')
	
	#Read in external data source for monthly unemployment rate
	unemp = pd.read_csv("UnemploymentRate.csv")
	#Change Month columns to a single identifier variable
	unemp = pd.melt(unemp, id_vars = ["Year"], value_vars= ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
	unemp["MonthNum"] = unemp.variable.apply(lambda x: datetime.datetime.strptime(x, "%b").strftime("%m"))
	unemp["YearMonth"] = unemp.Year.map(str) + "/"+ unemp.MonthNum
	
	#Drop year column to prevent duplication
	unemp = unemp.drop(axis=1, labels=["Year"])
	
	#Convert to Spark DataFrame
	unempdf = sqlContext.createDataFrame(unemp)
	
	
	df2 = df.select("ID", 'Date','Year', 'IUCR', 'Beat')
	df3 = df2.withColumn("Month", df.Date.substr(0,2))
	df4 = df3.withColumn("WeekNum", weekofyear(from_unixtime(unix_timestamp(df3.Date.substr(0,10),"MM/dd/yyyy"))))
	
	#Convert dataframe to weekly counts by beat
	df5 = df4.groupby("Beat", "Year", "WeekNum").count()
	
	#Partition by beat and create weekly lag variables
	WindowSpec = Window.partitionBy("Beat").orderBy("Year", "WeekNum")
	
	df6 = df5.withColumn("lag1", lag(col("count"),1).over(WindowSpec)) \
            .withColumn("lag2", lag(col("count"),2).over(WindowSpec)) \
            .withColumn("lag3", lag(col("count"),3).over(WindowSpec)) \
            .withColumn("lag4", lag(col("count"),4).over(WindowSpec)) \
            .withColumn("lag5", lag(col("count"),5).over(WindowSpec)) \
            .withColumn("lag10", lag(col("count"),10).over(WindowSpec))
        
	df6 = df6.withColumn("WeekYear", concat(df6.Year.cast("string"), lit("/"), df6.WeekNum.cast("string")))
	df6 = df6.withColumn("Month",  month(from_unixtime(unix_timestamp(df6.WeekYear,"yyyy/w"))))
	df6 = df6.withColumn("YearMonth", concat(df6.Year.cast("string"), lit("/"), df6.Month.cast("string")))
	
	#Join to get unemployment statistics from the external source
	df7 = df6.join(unempdf, "YearMonth")
	
	#Prepare dataframe for model
	assembler = VectorAssembler(inputCols=["Beat", "value", "Year", "WeekNum", "lag1", "lag2", "lag3",
                                      "lag4", "lag5", "lag10"], outputCol = "features")
	
	#Drop nulls
	modelDF = assembler.transform(df7.dropna())
	#Cast label to double for regression model
	modelDF = modelDF.withColumn("label", modelDF["count"].cast("double"))
	
	#Split train and test set
	(train, test) = modelDF.randomSplit([0.8, 0.2])
	
	#Build Random Forest Model
	rf_mod = RandomForestRegressor(featuresCol="features", labelCol="label", numTrees=100, maxDepth=4, maxBins=40)
	fitted = rf_mod.fit(train)
	
	#Get predictions for test set, and round them to integer values because it's a count.
	predictions = fitted.transform(test)
	predictions = predictions.withColumn("predictions",  round(predictions.prediction,0))
	evaluator = RegressionEvaluator(predictionCol="predictions", labelCol="label", metricName="r2")
	
	pred = predictions.select("label","predictions","features").toPandas()
	#Save predictions
	pred.to_csv('gupta_3_predictions.csv', index=False)
	with open('gupta_3.txt', 'w') as output:
		output.write("Test R-Squared = " + str(evaluator.evaluate(predictions)))
	output.close()