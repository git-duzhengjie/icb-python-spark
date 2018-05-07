# coding:utf-8
# python version: 
# author: duzhengjie
# date: 2017/12/29
# description:
# ©成都爱车宝信息科技有限公司版权所有
import getopt
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
    centenarians = sc.sql("SELECT DeviceCode, Data FROM tmp")
    return centenarians.rdd


def map_fun(r):
    """
    从获取的数据中生成设备号为key，经纬度为value的新的键值对
    注意：这里需要剔除经纬度为0的脏数据
    :param r:
    :return:
    """
    try:
        array = []
        for s in r.Data:
            if int(s.Gps.Coordinates[0]) != 0 and int(s.Gps.Coordinates[1]) != 0:
                array.append((s.Gps.Coordinates[0], s.Gps.Coordinates[1]))
        return r.DeviceCode, array
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


def find_minim(x, y):
    x0 = list(zip(*x))
    y0 = list(zip(*y))
    min_long_x = min(x0[0])
    min_lat_x = min(x0[1])
    min_long_y = min(y0[0])
    min_lat_y = min(y0[1])
    return [(min_long_x if min_long_x < min_long_y else min_long_y, min_lat_x if min_lat_x < min_lat_y else min_lat_y)]


def find_maxim(x, y):
    x0 = list(zip(*x))
    y0 = list(zip(*y))
    max_long_x = max(x0[0])
    max_lat_x = max(x0[1])
    max_long_y = max(y0[0])
    max_lat_y = max(y0[1])
    return [(max_long_x if max_long_x > max_long_y else max_long_y, max_lat_x if max_lat_x > max_lat_y else max_lat_y)]


def cal_mm():
    """
    计算每个设备的最小/最大经纬度
    :return:
    """
    minimum_result = map_rdd.reduceByKey(find_minim).map(lambda x: (x[0], x[1][0]))
    maximum_result = map_rdd.reduceByKey(find_maxim).map(lambda x: (x[0], x[1][0]))
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
    a += b
    return a


def rm_dirty(v):
    try:
        print(colored(v, "yellow"))
        return len(v) == 2 and str(v[0]) != '0000'
    except IndexError:
        return False


def usage():
    print(colored("Usage:python %s --lop= --lap=" % __file__, "red"))
    sys.exit(-1)


def check():
    flag = 0
    try:
        options, args = getopt.getopt(sys.argv[1:], "ho:a:", ["help", "lop=", "lap="])
    except getopt.GetoptError:
        usage()
    for name, value in options:
        if name in ("--help",):
            usage()
        if name in ("--lop",):
            global long_step
            long_step = float(value)
            print("longitude step is---- ", value)
            flag += 1
        if name in ("--lap",):
            print('latitude step is----', value)
            global lat_step
            lat_step = float(value)
            flag += 1
    if flag != 2:
        usage()


if __name__ == '__main__':
    import sys
    long_step = None
    lat_step = None
    check()
    sc = SparkSession.builder.appName("block count")  \
        .config("spark.mongodb.input.uri",
                "mongodb://192.168.0.230:27117/icb.LocationData?readPreference=primaryPreferred") \
        .config("spark.mongodb.output.uri", "mongodb://192.168.0.230:27117/warehouse.UserLocationCount") \
        .getOrCreate()
    rdd = get_rdd()
    map_rdd = rdd.map(map_fun)
    map_rdd_broadcast = map_rdd.reduceByKey(reduce_fun_append).collectAsMap()
    m_rdd = cal_mm()
    out = m_rdd.map(block_fun).map(count).filter(rm_dirty)
    sc.createDataFrame(out).write.format("com.mongodb.spark.sql").mode('overwrite').save()
