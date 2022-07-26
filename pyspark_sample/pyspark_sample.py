import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from typing import List

from NovelSubject import NovelSubject
from typing import List

import logging
import sys


def get_logger(name, level=logging.INFO):
    __logger = logging.getLogger(name)
    __logger.setLevel(level)
    if __logger.handlers:
        # or else, as I found out, we keep adding handlers and duplicate messages
        pass
    else:
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        __logger.addHandler(ch)
    return __logger


"""
    set("spark.driver.host", "10.240.150.172") : spark 를 외부 에서 접속시 spark 에서 결과 값을 전달할 host 지정
"""
conf: SparkConf = SparkConf().setAppName('Spark CSV Load Sample').setMaster('spark://localhost:7077') \
    .set("spark.driver.host", "10.240.140.25")

sc: SparkContext = SparkContext(conf=conf)
sc.setLogLevel("INFO")

log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.info("pyspark script logger initialized")

spark: SparkSession = SparkSession.builder.getOrCreate()

print("스파크 컨텍스트 버젼: ", sc.version)
print("Spark Context 파이썬 버전:", sc.pythonVer)
print("Spark Context 마스터:", sc.master)

"""
    spark.read.format('csv').load('airtravel.csv')는 FileNotFoundException이 발생하는것으로 봐서 spark master 또는
    worker 서버의 local 파일을 읽어오는 것으로 보인다 해당 로직을 사용하지 말것
"""

"""
pd_file_reader: pandas.io.parsers.readers.TextFileReader = pd.read_csv('file:airtravel.csv')
df: pyspark.sql.dataframe = spark.createDataFrame(pd_file_reader)
df.printSchema()
df.show()
"""

# df = spark.createDataFrame([(1, 21), (2, 30)], ("id", "age"))
# def filter_func(iterator):
#     for pdf in iterator:
#         yield pdf[pdf.id == 1]
# df.mapInPandas(filter_func, df.schema).show()


# convert NovelSubject list to data frame
novel_subject_list: List[NovelSubject] = [NovelSubject('소설제목1', 'url1'), NovelSubject('소설제목2', 'url2')]
json_list: list = list(map(lambda row: row.json_dict(), novel_subject_list))
data_frame: pyspark.sql.dataframe = spark.createDataFrame(json_list)
data_frame.show()


def __test(obj):
    logger = get_logger('worker')
    logger.info(obj)


data_frame.foreach(__test)
