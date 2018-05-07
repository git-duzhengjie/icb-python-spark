# coding:utf-8
# python version: 
# author:duzhengjie
# date: 2017/12/28
# description:
# ©成都爱车宝信息科技有限公司版权所有
from pyspark.sql import SparkSession


def map_fun(r):
    try:
        if (r.Gps.Coordinates[0], r.Gps.Coordinates[1]) != (0.0, 0.0):
            return r.DeviceCode, (r.Gps.Coordinates[0], r.Gps.Coordinates[1])
        return '0000', (0, 0)
    except TypeError:
        return '0000', (0, 0)


def get_rdd():
    df = spark.read.format("com.mongodb.spark.sql").load()
    df.registerTempTable("tmp")
    centenarians = spark.sql("SELECT DeviceCode, Gps FROM tmp")
    return centenarians.rdd


if __name__ == '__main__':
    spark = SparkSession.builder.appName("Python Spark SQL basic example") \
        .master("yarn") \
        .config("spark.mongodb.input.uri", "mongodb://192.168.0.235/ICB.DeviceData?readPreference=primaryPreferred") \
        .config("spark.mongodb.output.uri", "mongodb://192.168.0.235/warehouse.UserLocationCount") \
        .getOrCreate()
    rdd = get_rdd()
    rdd.map(map_fun).saveAsTextFile('/spark/output/device_ll')