import pyspark
from pyspark import SparkContext
import csv
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.stat import Statistics
import pandas as pd
import datetime
import scipy.stats as st
import math

'''Function to read comma-separated text accurately'''
def readerfn(x):
    csv_reader = csv.reader([x])
    rowfields = None
    for row in csv_reader:
        rowfields = row
    return rowfields
'''Function to create sparsevectors for each year'''
def mapfn(x, allbeatslist):
    btpos = []
    vals = []
    for tup in x[1]:
        btpos.append(allbeatslist.index(tup[0]))
        vals.append(tup[1])
    btpos.sort()
    return (x[0], SparseVector(len(allbeatslist), btpos, vals))

'''Function to identify which Mayor a crime took place under'''
def isDaley(x):
	crimedate = datetime.datetime.strptime(x[2][0:10], "%m/%d/%Y")
	mayordate = datetime.datetime.strptime("05/16/2011", "%m/%d/%Y")
	if crimedate < mayordate:
		return True
	return False

if __name__ == '__main__':
	sc = SparkContext()
	sc.setLogLevel("ERROR")
	#Read in data
	myRDD = sc.textFile("CrimeFolder/Crimes_-_2001_to_present.csv")
	#Read in comma-separated data correctly. 
	myRDDnew = myRDD.map(readerfn)
	#Filter out first line of data
	myRDD1 = myRDDnew.first()
	myRDD2 = myRDDnew.filter(lambda x: x != myRDD1)
	
	#Separate the two mayors
	daleyRDD = myRDD2.filter(lambda x: isDaley(x))
	rahmRDD = myRDD2.filter(lambda x: not (isDaley(x)))
	
	#Give the additional weight to 2015 crimes to correct average figures.
	daleyByYear = daleyRDD.map(lambda x: (x[17], 12/5.5 if x[17] == '2015' else 1))
	rahmByYear = rahmRDD.map(lambda x: (x[17], 12/6.5 if x[17] == '2015' else 1))
	
	daleyGroupedYr = daleyByYear.reduceByKey(lambda x, y : x + y)
	rahmGroupedYr = rahmByYear.reduceByKey(lambda x, y : x + y)
	
	#All crimes under each  mayor
	daleyTotal = daleyGroupedYr.map(lambda x: x[1]).reduce(lambda x,y: x + y)
	rahmTotal = rahmGroupedYr.map(lambda x: x[1]).reduce(lambda x,y: x + y)
	
	#Total number of years under each mayor
	daleyYears = daleyGroupedYr.count()
	rahmYears = rahmGroupedYr.count()
	
	#Mean crimes/year under each mayor
	daleyMean = daleyTotal/daleyYears
	rahmMean = rahmTotal/rahmYears
	
	#Standard Deviation of crimes/year under each mayor
	daleySD = daleyGroupedYr.map(lambda x: x[1]).stdev()
	rahmSD = rahmGroupedYr.map(lambda x: x[1]).stdev()
	
	#Calculate for difference in means.
	zscore = (daleyMean - rahmMean)/(math.sqrt((((daleySD)**2)/daleyYears) + ((rahmSD**2)/rahmYears)))
	alpha = st.norm.cdf(zscore)
	
	with open("Gupta_2c.txt", "w") as output:
		output.write("The z-score for difference in yearly total crime between Daley and Rahm is: " + str(zscore) + '\n')
		output.write("The alpha for difference in yearly total crime between Daley and Rahm is: " + str(alpha) +
					 "...\nindicating that the difference in crime between the two mayors is statistically significant, with more crime under Mayor Daley.")
    

