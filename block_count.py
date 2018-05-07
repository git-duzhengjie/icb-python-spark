# coding:utf-8
# python version: 
# author:duzhengjie
# date: 2017/12/28
# description:
# ©成都爱车宝信息科技有限公司版权所有
from pyspark import SparkContext, broadcast
import numpy as np
from termcolor import colored
import itertools


def print_f(x):
    print(colored(x, "blue"))


def serialized(x):
    try:
        return tuple(eval(x))
    except SyntaxError:
        return '0000', (0, 0)


def reduce_fun_append(a, b):
    if type(a) is not list:
        a = [a]
    a.append(b)
    return a


long_step = None
lat_step = None
sc = SparkContext(master="spark://master:7077", appName="block_count")
map_rdd = sc.wholeTextFiles('/spark/output/device_ll').map(lambda r: r[1]).flatMap(lambda r: r.split('\n')) \
    .map(serialized).reduceByKey(reduce_fun_append)
map_rdd = sc.broadcast(map_rdd.collectAsMap())
# print(colored(map_rdd.value, "yellow"))


def count(v):
    try:
        if v is None:
            return ()
        block = list(itertools.product(v[1][0], v[1][1]))
        result = []
        for b in block:
            ct = 0
            for r in map_rdd.value.get(v[0], []):
                ls = b[0] + long_step
                ts = b[1] + lat_step
                if b[0] <= r[0] < ls and b[1] <= r[1] < ts:
                    ct += 1
            result.append((b, ct))
        return v[0], result
    except TypeError:
        return '0000', []
    except ValueError:
        return '0000', []


def count_block(a, map_rdd):
    result = []
    ct = 0
    for r in map_rdd.value.get(a[0], []):
        if a[1][0] <= r[0] < a[1][0] + long_step and a[1][1] <= r[1] < a[1][1] + lat_step:
            ct += 0
    result.append((a[1], ct))
    return result


def block_fun(v):
    try:
        return (v[0], (list(np.arange(v[1][0][0], v[1][1][0], long_step)),
                       list(np.arange(v[1][0][1], v[1][1][1], lat_step))))
    except TypeError:
        return None


def main():
    rdd = sc.wholeTextFiles('/spark/output/device_mm').map(lambda r: r[1]).flatMap(lambda r: r.split('\n')) \
        .map(serialized).map(block_fun).map(count)
    # print(colored(rdd[:2], "yellow"))
    rdd.saveAsTextFile('/spark/output/block_count')


if __name__ == '__main__':
    import sys

    if len(sys.argv) != 3:
        print(colored("usage:%s longitude_step latitude_step" % __file__, "red"))
        sys.exit(-1)
    global long_step
    long_step = float(sys.argv[1])
    global lat_step
    lat_step = float(sys.argv[2])
    main()
