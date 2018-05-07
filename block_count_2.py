# coding:utf-8
# python version: 
# author:duzhengjie
# date: 2017/12/29
# description:
# ©成都爱车宝信息科技有限公司版权所有
import json

from pyspark.sql import SparkSession
from termcolor import colored
import numpy as np
import itertools


def get_rdd():
    """
    从mongodb获取设备位置数据，并生成rdd
    :return:
    """
    df = sc.read.format("com.mongodb.spark.sql").load()
    df.registerTempTable("tmp")
    centenarians = sc.sql("SELECT DeviceCode, Gps FROM tmp")
    return centenarians.rdd


def map_fun(r):
    """
    从获取的数据中生成设备号为key，经纬度为value的新的键值对
    注意：这里需要剔除经纬度为0的脏数据
    :param r:
    :return:
    """
    try:
        if int(r.Gps.Coordinates[0]) != 0 and int(r.Gps.Coordinates[1]) != 0:
            return r.DeviceCode, (r.Gps.Coordinates[0], r.Gps.Coordinates[1])
        return '0000', (0, 0)
    except TypeError:
        return '0000', (0, 0)


def merge_rdd(rdd0, rdd1):
    """
    将两个rdd根据key进行合并
    :param rdd0:
    :param rdd1:
    :return:
    """
    return rdd0.join(rdd1)


def cal_mm():
    """
    计算每个设备的最小/最大经纬度
    :return:
    """
    minimum_result = map_rdd.reduceByKey(lambda x, y: (y[0] if x[0] > y[0] else x[0], y[1] if x[1] > y[1] else x[1]))
    maximum_result = map_rdd.reduceByKey(lambda x, y: (y[0] if x[0] < y[0] else x[0], y[1] if x[1] < y[1] else x[1]))
    return merge_rdd(minimum_result, maximum_result)


def block_fun(v):
    """
    根据最小最大经纬度以及经纬度步长信息生成两个数组，用于生成最后的经纬度块的笛卡儿积
    :param v:
    :return:
    """
    try:
        return (v[0], (list(np.arange(v[1][0][0], v[1][1][0], long_step)),
                       list(np.arange(v[1][0][1], v[1][1][1], lat_step))))
    except TypeError:
        return None


def count(v):
    """
    统计每个设备的经纬度块出现的次数
    :param v:
    :return:
    """
    try:
        if v is None or v[0] == "0000":
            return ()
        block = list(itertools.product(v[1][0], v[1][1]))
        result = []
        for b in block:
            ct = 0
            for r in map_rdd_broadcast.get(v[0], []):
                ls = b[0] + long_step
                ts = b[1] + lat_step
                if b[0] <= r[0] < ls and b[1] <= r[1] < ts:
                    ct += 1
            result.append((b, ct))
        return v[0], json.dumps(result)
    except TypeError:
        return '0000', []
    except ValueError:
        return '0000', []


def reduce_fun_append(a, b):
    if type(a) is not list:
        a = [a]
    a.append(b)
    return a


def rm_dirty(v):
    try:
        print(colored(v, "yellow"))
        return len(v) == 2 and str(v[0]) != '0000'
    except IndexError:
        return False


if __name__ == '__main__':
    import sys

    if len(sys.argv) != 3:
        print(colored("usage:%s longitude_step latitude_step" % __file__, "red"))
        sys.exit(-1)
    long_step = float(sys.argv[1])
    lat_step = float(sys.argv[2])
    print(colored('long {0}, lat {1}'.format(long_step, lat_step), "red"))
    sc = SparkSession.builder.appName("Python Spark SQL basic example") \
        .master("yarn") \
        .config("spark.mongodb.input.uri", "mongodb://192.168.0.235/ICB.DeviceData?readPreference=primaryPreferred") \
        .config("spark.mongodb.output.uri", "mongodb://192.168.0.235/warehouse.UserLocationCount") \
        .getOrCreate()
    rdd = get_rdd()
    map_rdd = rdd.map(map_fun)
    map_rdd_broadcast = map_rdd.reduceByKey(reduce_fun_append).collectAsMap()
    m_rdd = cal_mm()
    out = m_rdd.map(block_fun).map(count).filter(rm_dirty)
        # .saveAsTextFile('/spark/output/block_count_2_8')
    sc.createDataFrame(out).write.format("com.mongodb.spark.sql").mode('overwrite').save()
