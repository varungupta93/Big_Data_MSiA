import pyspark
from pyspark import SparkContext
import csv
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.stat import Statistics
import pandas as pd

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


if __name__ == '__main__':
	sc = SparkContext()
	sc.setLogLevel("ERROR")
	#Read in data
	myRDD = sc.textFile("CrimeFolder/Crimes_-_2001_to_present.csv")
	#Read in csv data correctly
	myRDDnew = myRDD.map(readerfn)
	#Remove header row
	myRDD1 = myRDDnew.first()
	myRDD2 = myRDDnew.filter(lambda x: x != myRDD1 )

	#Filter out data not in last 5 years
	yearsRDD = myRDD2.map(lambda x: x[17]).distinct()
	last5 = yearsRDD.top(5, key = lambda x: x)
	filteredRDD = myRDD2.filter(lambda x: x[17] in last5)

	myRDD3 = filteredRDD.map(lambda x: ((x[10],x[17]),1) ).reduceByKey(lambda x, y: x + y)
	#Remove blank beats, rearrange to keep only year as key.
	myRDD4 = myRDD3.filter(lambda x: x[0][0] != '').map(lambda x: (x[0][1], [(x[0][0], x[1])]))
	myRDD5 = myRDD4.sortBy(lambda x: x[0])
	myRDD6 = myRDD5.reduceByKey(lambda x,y: x + y)
	
	#Get a list of all beats
	allbeats = myRDD4.map(lambda x: x[1][0][0]).distinct().sortBy(lambda x: x)
	allbeatslist = allbeats.collect()
	
	#Get sparse vectors for each year
	myRDD7 = myRDD6.sortBy(lambda x: x[0]).map(lambda x: mapfn(x,allbeatslist))
	#Get correlation matrix of beats
	cormat = Statistics.corr(myRDD7.map(lambda x: x[1]), method="pearson")

	#Standard analysis on pandas
	pd_df = pd.DataFrame(cormat, index=allbeatslist, columns=allbeatslist)
	listvals = pd_df.unstack().reset_index()
	#Sort by correlation
	listvals = listvals.sort_values(by = listvals.columns[2], ascending=False)
	#Remove correlations with self
	listvals2 = listvals[listvals.level_0 != listvals.level_1]
	#Remove reverse correlations, which are basically duplicates
	listvals2["setcol"] = listvals2.apply(lambda row: ''.join(sorted([row['level_0'], row['level_1']])), axis=1)
	listvals2 = listvals2.drop_duplicates("setcol")
	listvals2 = listvals2[['level_0', 'level_1', listvals2.columns[2]]]
	listvals2.columns = ['Beat1', 'Beat2', 'Correlation']
	removebeats = ["13..","23..", "21.."]
	listvals2 = listvals2[~listvals2.Beat1.str.contains("|".join(removebeats))] 
	listvals2 = listvals2[~listvals2.Beat2.str.contains("|".join(removebeats))] 
	#Output to csv
	listvals2.to_csv("Gupta_2b.txt", header = True, sep = ',', index = False)