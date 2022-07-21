import pandas.io.parsers.readers
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

import pandas as pd

conf: SparkConf = SparkConf().setAppName('Spark CSV Load Sample').setMaster('spark://localhost:7077')

sc: SparkContext = SparkContext(conf=conf)
sc.setLogLevel("INFO")

spark: SparkSession = SparkSession.builder.getOrCreate()

print("스파크 컨텍스트 버젼: ", sc.version)
print("Spark Context 파이썬 버전:", sc.pythonVer)
print("Spark Context 마스터:", sc.master)

"""
    spark.read.format('csv').load('airtravel.csv')는 FileNotFoundException이 발생하는것으로 봐서 spark master 또는
    worker 서버의 local 파일을 읽어오는 것으로 보인다 해당 로직을 사용하지 말것
"""

pd_file_reader: pandas.io.parsers.readers.TextFileReader = pd.read_csv('file:airtravel.csv')
sparkDF: pyspark.sql.dataframe = spark.createDataFrame(pd_file_reader)
sparkDF.printSchema()


