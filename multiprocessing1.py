from pyspark.sql import *
from pyspark import SparkContext, SparkConf
import threading
import pyspark
from pyspark.sql.functions import *
import findspark
import time
import concurrent.futures
findspark.init()

conf = pyspark.SparkConf().setAppName('appName').setMaster(
    'local').set('spark.scheduler.mode', 'FAIR')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)


def test(a):
    z = ("NA", "NA")

    if a == "A":
        sc.setLocalProperty("spark.scheduler.pool", a)
        print("thread A started at " + str(time.time()))
        range100 = spark.range(100)
        count100 = range100.count()
        z = (a, str(count100))
    elif a == "B":

        sc.setLocalProperty("spark.scheduler.pool", a)
        print("thread B started at " + str(time.time()))
        range200 = spark.range(100000)
        count200 = range200.count()
        z = (a, str(count200))
    elif a == "C":
        sc.setLocalProperty("spark.scheduler.pool", a)
        print("thread C started at " + str(time.time()))
        range300 = spark.range(300)
        count300 = range300.count()
        z = (a, str(count300))
    print("thread {} ended at ".format(a) + str(time.time()))
    sc.setLocalProperty("spark.scheduler.pool", None)
    return z


def main():
    arr = ["A", "B", "C"]

    start_time = time.time()
    result = []
    # for i in arr:
    #     result.append(test(i))
    with concurrent.futures.ProcessPoolExecutor(max_workers=1) as executor:
        # result = executor.map(test, arr)
        result_futures = list(map(lambda x: executor.submit(test, x), arr))
        # result = [Row(f.result())
        #           for f in concurrent.futures.as_completed(result_futures)]
        for _ in concurrent.futures.as_completed(result_futures):
            print('Result: ', _.result())
            result.append(_.result())

    print(result)
    result_df = spark.sparkContext.parallelize(result).toDF()
    print(result_df.show())
    print(start_time-time.time())
    # result = p.map(test, arr)
    # p.close()
    # p.join()


if __name__ == "__main__":

    main()
