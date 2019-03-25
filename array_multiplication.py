#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

from functools import partial

# A: L x M
# B: M x N

L = 10000
M = 1000
N = 5000


def read_A(line):
	fields = line.split(',')
	i, j, v  = fields[0], fields[1], fields[2]
	return int(i), (int(j), int(v))

def read_B(line):
	fields = line.split(',')
	i, j, v  = fields[0], fields[1], fields[2]
	return int(j), (int(i), int(v))

def inner_product(vec1, vec2):
	s = 0
	for ((_, v1), (_, v2)) in zip(vec1, vec2):
		s += v1*v2
	return s	

if __name__ == "__main__":

	conf = SparkConf().setAppName('MatMul').setMaster('spark://master:7077')
	sc = SparkContext(conf=conf)	
	
	AFile = sc.textFile('hdfs://master:9000/A.csv', 100)
	BFile = sc.textFile('hdfs://master:9000/B.csv', 100)

	sort_vector = partial(sorted, key=lambda v: v[0])
	
	A = AFile.map(read_A)\
		.groupByKey()\
		.map(lambda (k, v): (k, sort_vector(v)))
	B = BFile.map(read_B)\
		.groupByKey()\
		.map(lambda (k, v): (k, sort_vector(v)))

	C = A.cartesian(B)\
		.map(lambda ((row_A, vec1), (col_B, vec2)): ((row_A, col_B), inner_product(vec1, vec2)))
	C.coalesce(1, True).saveAsTextFile('hdfs://master:9000/C')


# k, (v_A, (col_B, v_B))
