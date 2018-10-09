from pyspark import SparkContext
import csv

def readerfn(x):
    csv_reader = csv.reader([x])
    rowfields = None
    for row in csv_reader:
        rowfields = row
    return rowfields

if __name__ == '__main__':
	sc = SparkContext()
	myRDD = sc.textFile("CrimeFolder/Crimes_-_2001_to_present.csv")
	#Read in csv correctly
	myRDDnew = myRDD.map(readerfn)
	#Remove header row
	myRDD1 = myRDDnew.first()
	myRDD2 = myRDDnew.filter(lambda x: x != myRDD1 )
	
	#Get last 3 years and filter data for only those 3.
	yearsRDD = myRDD2.map(lambda x: x[17]).distinct()
	last3 = yearsRDD.top(3, key = lambda x: x)
	filteredRDD = myRDD2.filter(lambda x: x[17] in last3)
	
	#Get counts in those years by block.
	myRDD3 = filteredRDD.map(lambda x: (x[3],1) ).reduceByKey(lambda x, y: x + y)
	#Take top 10 and output.
	myRDD4 = myRDD3.top(10, key = lambda x: x[1])
	with open("Gupta_2a.txt", "w") as output:
		for t in myRDD4:
			output.write(','.join(str(s) for s in t) + '\n')
