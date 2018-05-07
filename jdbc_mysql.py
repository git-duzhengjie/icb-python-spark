# coding:utf-8
# python version: 
# author:duzhengjie
# date: 2018/1/2
# description:
# ©成都爱车宝信息科技有限公司版权所有
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("mysql example").master("yarn").getOrCreate()
    hostname = '192.168.0.250'
    db = 'db1101'
    port = 3306
    user = 'root'
    password = 'icb@888'
    jdbc_url = "jdbc:mysql://{0}:{1}/{2}?user={3}&password={4}".format(hostname, port, db, user, password)
    df = spark.read.jdbc(url=jdbc_url, table='t_friends')
    df.printSchema()