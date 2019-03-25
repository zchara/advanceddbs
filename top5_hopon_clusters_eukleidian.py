#!/usr/bin/env python
# coding: utf-8

from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

import math    

k = 5
MAX_ITERATIONS = 3

def distance(origin, destination):
	lat1, lon1 = origin
	lat2, lon2 = destination
	return ((lat1-lat2)**2 + (lon1-lon2)**2)**0.5

def min_dist(point, centroids):
	min_distance = distance(point, centroids[0])
	arg_min = 0
	for i in range(1,k):
		dist = distance(point, centroids[i])
		if(dist < min_distance):
			min_distance = dist
			arg_min = i

	return arg_min, (point[0], point[1], 1) 


def start_pair(line):
	fields = line.split(',')
	start_long, start_lat = fields[3:5]
	return float(start_long), float(start_lat)

if __name__ == "__main__":

	
	conf = SparkConf().setAppName('KMeans').setMaster('spark://master:7077')
	sc = SparkContext(conf=conf)	
	tripFile = sc.textFile('hdfs://master:9000/yellow_tripdata_1m.csv')
	points = tripFile.map(start_pair)

	centroids = points.take(k)
	print('Centroids', centroids)

	for i in range(MAX_ITERATIONS):
		centroids = points.map(lambda point: min_dist(point, centroids))\
			.reduceByKey(lambda agg1, agg2: (
				(agg1[0] + agg2[0]), 
				(agg1[1] + agg2[1]), 
				(agg1[2] + agg2[2])))\
			.map(lambda (k,(lon, lat, c)): (lon/c, lat/c))\
			.coalesce(1, True)\
			.collect()
		print(centroids)
	
	sc.parallelize(centroids, 1).saveAsTextFile('hdfs://master:9000/centroids_dummy')



