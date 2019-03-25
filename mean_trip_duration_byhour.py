#!/usr/bin/env python
# coding: utf-8

from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from datetime import datetime

def date_duration_pair(line):
    fields = line.split(',')
    dep_date = datetime.strptime(fields[1], '%Y-%m-%d %H:%M:%S')
    arr_date = datetime.strptime(fields[2], '%Y-%m-%d %H:%M:%S')
    return dep_date.hour, (float((arr_date - dep_date).total_seconds())/60, 1)
    

if __name__ == "__main__":

	
	conf = SparkConf().setAppName('MeanDurr').setMaster('spark://master:7077')
	sc = SparkContext(conf=conf)	
	tripFile = sc.textFile('hdfs://master:9000/yellow_tripdata_1m.csv')

	mean_dur = tripFile.map(lambda line: date_duration_pair(line))\
			.reduceByKey(lambda agg1,agg2: ((agg1[0] + agg2[0]), (agg1[1] + agg2[1])))\
	                .map(lambda (k,(s,c)): (k,s/c))\
			.coalesce(1, True)\
	                .sortByKey(True)

	mean_dur.saveAsTextFile('hdfs://master:9000/mean_duration')

