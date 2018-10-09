from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql.functions import countDistinct, col, lit, min, concat, date_format, to_date
import numpy as np
import pandas as pd
from math import sqrt
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pyspark.mllib.clustering import KMeansModel, KMeans
import datetime
from pyspark.mllib.feature import StandardScaler, Vectors, DenseVector

import csv
import re


def readerfn(x):
    csv_reader = csv.reader([x])
    rowfields = None
    for row in csv_reader:
        rowfields = row
    return rowfields

def getDescFeatures(desc, emoji_regex):
    emojis = re.findall(emoji_regex, desc)
    
    utilemojis = "💡🔌⚡🚘🚗"
    utilcount = sum([desc.count(em) for em in utilemojis])
    
    foodemojis = "🍴🍽🍳🍕🎂🌮"
    foodcount = sum([desc.count(em) for em in foodemojis])
    
    numchars = len(desc)
    
    numwords = len(desc.split())
    if numwords >  0:
	    maxword = max([len(wrd) for wrd in desc.split()])
    else:
	    maxword = 0
    
    return (len(emojis), utilcount, foodcount, numchars, numwords, maxword)


if __name__ == '__main__':
	sc = SparkContext()
	sc.setLogLevel("ERROR")
	sqlContext = SQLContext(sc)
	myRDD = sc.textFile('VenmoDir/venmoSample.csv')
	myRDDnew = myRDD.map(readerfn)

	myRDD1 = myRDDnew.first()
	myRDD2 = myRDDnew.filter(lambda x: x != myRDD1)

	emoji_regex = r'[^\w\s,.\'\";:@#\\$%&!)(-/\x00\uFE0F]'

	myRDD3 = myRDD2.map(lambda x: x[4])
	myRDD4 = myRDD3.map(lambda x: getDescFeatures(x, emoji_regex))

	scaler = StandardScaler(withMean = True, withStd = True).fit(myRDD4)
	scaledRDD = scaler.transform(myRDD4).map(lambda x: (x[0],x[1], x[2], x[3], x[4], x[5]))

	numclusts = [2,3,4,5,6]
	cluster_centers = []
	SSEs = []
	
	for numclust in numclusts:
	    kmeans_clusts = KMeans.train(scaledRDD, numclust, maxIterations=10, seed=12345)
	    def getSSE(pt):
	        center = kmeans_clusts.centers[kmeans_clusts.predict(pt)]
	        return sqrt(np.sum([x**2 for x in (pt - center)]))
	    SSE = scaledRDD.map(lambda pt: getSSE(pt)).reduce(lambda x, y: x + y)
	    
	    SSEs.append(SSE)
	    cluster_centers.append(kmeans_clusts.centers)


	Scree plot with SSE
	plt.gcf().clear()
	plt.plot(numclusts, SSEs)
	plt.xticks(numclusts)
	plt.ylabel("Intra-Cluster SSEs")
	plt.xlabel("Number of Clusters")
	plt.title("SSE Plot for Elbow")
	plt.savefig('Exercise2_SSEs.png', bbox_inches="tight")
	plt.close()
	
	
	used_clust_centers = cluster_centers[0] #[3]
	# write cluster means to txt
	dimensions = ['Number of Emojis','Utility Emojis','Food Emojis','Number of Characters','Number of Words', 'Largest Word Length']
	with open("Exercise2_centroids.txt", "w") as output:
		for clustNum in range(len(used_clust_centers)):
			output.write('\n\n__Cluster Number '+ str(clustNum +1)+'__\n')
			for i in range(len(dimensions)):
				output.write(dimensions[i]+'  :  '+str(used_clust_centers[clustNum][i])+'\n')
		


	

