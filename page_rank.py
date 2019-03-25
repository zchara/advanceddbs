#!/usr/bin/env python
# coding: utf-8

from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

N = 875713
d = 0.85
MAX_ITERATIONS = 5
INIT_SCORE = 0.5

def page_pair(line):
	fields = line.split('\t')
	dep_page, dest_page  = fields[0], fields[1]
	return int(dep_page), int(dest_page)

if __name__ == "__main__":

	
	conf = SparkConf().setAppName('PageRank').setMaster('spark://master:7077')
	sc = SparkContext(conf=conf)

	edgesFile = sc.textFile('hdfs://master:9000/web-Google.txt')

	edges = edgesFile.map(page_pair)
	L = edges.map(lambda (k, v): (k, 1)).reduceByKey(lambda v1, v2: v1+v2) #se posous deixnei h selida	
	#print(L.collect())
	M = edges.map(lambda (k, v): (v, k))

	PR = edges.map(lambda (k, v): (k, INIT_SCORE)).distinct()

	for i in range(MAX_ITERATIONS):
		#gia kathe selida vriskoyme to klasma PR/L
		#prosthetoume ston arxiko pinaka thn plhroforia tou klasmatos
		#kratame ws key to pou deixnei mia selida 
		#gia kathe selida vriskoyme to sum klasmaton ton selidon poy deixnoyn se ekeini
		#telos, oloklhronoume ton typo toy PR
		klasma = PR.join(L).map(lambda (k, (pr, l)): (k, pr/l)) 
		PR = edges.join(klasma)\
					.map(lambda (pj, (pi, klasmaa)): (pi, klasmaa))\
					.reduceByKey(lambda k1, k2: k1 + k2)\
					.map(lambda (k, sum_): (k, (1 - d)/N + d*sum_))


	#sorted_PR = PR.map(lambda (k, v): (v, k))\
	#	.coalesce(1, True)\
	#	.sortByKey(False)
	
	PR.saveAsTextFile('hdfs://master:9000/page_rank')


