from pyspark import SparkConf, SparkContext
import time

conf = SparkConf().setAppName('Test App')
sc = SparkContext(conf=conf)

s_time = time.perf_counter()

count = sc.range(1, 1000 * 1000 * 100).filter(lambda x: x > 100).count()
print('count: ', count)
e_time = time.perf_counter()
print("Time Cost: ", e_time - s_time)
