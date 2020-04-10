from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import threading
import pyspark
import findspark
import time
findspark.init()

conf = pyspark.SparkConf().setAppName('appName').setMaster(
    'local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)


arr = ["A", "B", "C"]


def test(a):
    z = "NA"

    if a == "A":
        range100 = spark.range(100)
        count100 = range100.count()
        z = str(count100)

    elif a == "B":
        range200 = spark.range(100000)
        count200 = range200.count()
        z = str(count200)
    elif a == "C":
        range300 = spark.range(300)
        count300 = range300.count()
        z = str(count300)
    return z


result = []
start_time = time.time()
for i in arr:
    result.append(test(i))

print(result)
print(start_time-time.time())
#result = p.map(test, arr)
# p.close()
# p.join()
