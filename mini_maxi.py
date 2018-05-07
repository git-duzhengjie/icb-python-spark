# coding:utf-8
# python version: 3.6
# author:duzhengjie
# ©成都爱车宝信息科技有限公司版权所有
# date:2017/12/27
# description:计算每个设备的最大最小经纬度，并存入mongodb中
from pyspark import SparkContext
from pyspark.sql import SparkSession
import numpy as np
from termcolor import colored

spark = SparkSession.builder.appName("Python Spark SQL basic example") \
    .master("yarn") \
    .config("spark.mongodb.input.uri", "mongodb://192.168.0.235/ICB.DeviceData?readPreference=primaryPreferred") \
    .config("spark.mongodb.output.uri", "mongodb://192.168.0.235/warehouse.UserLocationCount") \
    .getOrCreate()

map_rdd = None
long_step = None
lat_step = None
map_rdd_collect = None


def map_fun(r):
    try:
        return r.DeviceCode, (r.Gps.Coordinates[0], r.Gps.Coordinates[1])
    except TypeError:
        return '0000', (0, 0)


def get_rdd():
    df = spark.read.format("com.mongodb.spark.sql").load()
    df.registerTempTable("tmp")
    centenarians = spark.sql("SELECT DeviceCode, Gps FROM tmp")
    return centenarians.rdd


def get_map(rdd):
    return rdd.map(map_fun)


def cal_mm():
    minimum_result = map_rdd.reduceByKey(lambda x, y: (y[0] if x[0] > y[0] else x[0], y[1] if x[1] > y[1] else x[1]))
    maximum_result = map_rdd.reduceByKey(lambda x, y: (y[0] if x[0] < y[0] else x[0], y[1] if x[1] < y[1] else x[1]))
    return minimum_result, maximum_result


def write_mongodb(out):
    characters = spark.createDataFrame(out)
    characters.write.format("com.mongodb.spark.sql").mode('overwrite').save()


def merge_rdd(rdd0, rdd1):
    return rdd0.join(rdd1)


def block_fun(v):
    return (v[0], (list(np.arange(v[1][0][0], v[1][1][0], long_step)),
                   list(np.arange(v[1][0][1], v[1][1][1], lat_step))))


def split(mini_max):
    return mini_max.map(block_fun)


def count_block(a, b):
    result = []
    ct = 0
    for r in map_rdd_collect.get(a[0], []):
        if a[1][0] <= r[0] < a[1][0] + long_step and a[1][1] <= r[1] < a[1][1] + lat_step:
            ct += 0
    result.append((a[1], ct))
    return result


def count(v):
    block = []
    for i in v[1][0]:
        for j in v[1][1]:
            block.append((i, j))
    rdd = sc.parallelize(block).map(lambda a: (a, (v[0], a)))
    rdd.reduceByKey(count_block)
    return rdd.collectAsMap()


def reduce_fun_append(a, b):
    if type(a) is not list:
        a = [a]
    a.append(b)
    return a


def main():
    rdd = get_rdd()
    global map_rdd
    map_rdd = get_map(rdd)
    # print(colored(map_rdd.lookup('868120175885828'), "red"))
    global map_rdd_collect
    map_rdd_collect = map_rdd.reduceByKey(reduce_fun_append).collectAsMap()
    min_rdd, max_rdd = cal_mm()
    join_rdd = min_rdd.join(max_rdd)
    # # join_rdd.saveAsTextFile('/spark/output/device_mm')
    # # print(colored(join_rdd_collect[0], "red"))
    block_rdd = split(join_rdd)
    block_rdd.saveAsTextFile('/spark/output/device_mm_list')
    # block_count = block_rdd.map(count)
    # block_count.saveAsTextFile('/spark/output/device11')
    spark.stop()


if __name__ == '__main__':
    main()
