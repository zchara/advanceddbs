from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from datetime import datetime

def id_pay_pair(line):
	fields = line.split(',')
	return fields[0], float(fields[7])


if __name__ == "__main__":


        conf = SparkConf().setAppName('MaxPayment').setMaster('spark://master:7077')
        sc = SparkContext(conf=conf)

        tripFile = sc.textFile('hdfs://master:9000/yellow_tripdata_1m.csv')
        companyFile = sc.textFile('hdfs://master:9000/yellow_tripvendors_1m.csv')
        
	#get (tripID, vendor)
	companies = companyFile.map(lambda line: line.split(','))

	max_pay = tripFile.map(id_pay_pair)\
			.join(companies)\
			.map(lambda (ID, (pay, company)): (company, pay))\
			.reduceByKey(lambda val1, val2: max(val1, val2))\
			.coalesce(1,True)\
			.sortByKey(True)
	
	max_pay.saveAsTextFile('hdfs://master:9000/max_pay')


