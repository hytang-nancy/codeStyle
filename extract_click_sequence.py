#!/usr/bin/env python
# coding=utf-8

#########################################################################
#
# Copyright (c) 2017 Tencent Inc. All Rights Reserved
#
#########################################################################


"""
Author: violatang@tencent.com
Ddate: 2017/08/07 10:08:41
Brief: extract click sequence of each user
"""

from pyspark import SparkContext
import sys
from optparse import OptionParser
from pyspark.storagelevel import StorageLevel
import json
import heapq
import datetime
import random
import hashlib
import time
import nameapi 
import redis
reload(sys)
sys.setdefaultencoding('utf-8')

CHAR_LEN = 16
N_HASH = 3000017

def get_sequence_1702(line):
    words = line.strip().split('\t')
    openId = words[0]
    articles = words[1].split('\1')
    res = []
    for article in articles:
        elements = article.split('\2')
        timestamp = elements[0]
        topnewsId = elements[1]
        cmsid = elements[2]
        pos = elements[3]
        res.append((openId, [(cmsid, timestamp)]))
    return res


# get user click sequence in 4960 and 4961
def get_sequence_4960_4961(line):
    words = line.strip().split('\t')
    openId = words[0]
    articles = words[1].split('\1')
    res = []
    for article in articles:
        elements = article.split('\2')
        timestamp = elements[0]
        topnewsId = elements[1]
        action = elements[2]
        cmsid = elements[3]
        pos = elements[4]
        if action != "91" or cmsid.strip() == "":
            continue
        res.append((openId, [(cmsid, timestamp)]))
    return res

N_NEWS = 20
def merge_sequence(openId, articles):
    articles_sort = sorted(articles, key=lambda x:int(x[1]), reverse=True)
    if len(articles_sort) > N_NEWS:
        res = [x[0] for x in articles_sort[0:N_NEWS]]
    else:
        res = [x[0] for x in articles_sort]
    sequence = ",".join(map(str, res))
    return openId + "\t" + sequence

REDIS_HOST="all.wxplugin_user_readHistory.ssdb.com"
REDIS_HDL = None
EXPIRE_TIME = 60*60*24*14 # two weeks
KEY_PREFIX = "WXHIS_"
# get redis handler
def rdsHandler():
    global REDIS_HDL
    if REDIS_HDL == None:
        try:
            s, ip, port = nameapi.getHostByKey(REDIS_HOST)
            REDIS_HDL = redis.StrictRedis(ip, port, db=0)
        except:
            pass     
    return REDIS_HDL

def add2redis(key, value):
    redisconn = rdsHandler()
    res = redisconn.setex(key, EXPIRE_TIME, value)
    return res

N_FLUSH = 10
def dump2redis(sequences):
    redisconn = rdsHandler()
    pipe = redisconn.pipeline()
    c = 0
    for sequence in sequences:
        try:
            uid,articles = sequence.split('\t')
        except:
            continue 
        pipe.setex(KEY_PREFIX+uid, EXPIRE_TIME, articles)
        c += 1
        if c % N_FLUSH == 0:
            pipe.execute()
            pipe.reset()


if __name__ == "__main__":
    sc = SparkContext(appName="extract click sequence of each user")

    op = OptionParser()
    op.add_option("--ip1702", "--inpath_1702", dest="inpath_1702", help="click sequence in 1702")
    op.add_option("--ip4960", "--inpath_4960", dest="inpath_4960", help="click sequence in 4960")
    op.add_option("--ip4961", "--inpath_4961", dest="inpath_4961", help="click sequence in 4961")
    op.add_option("--ip3620", "--inpath_3620", dest="inpath_3620", help="click sequence in 3620")
    op.add_option("--op", "--outpath", dest="outpath", help="output path")
    (options, args) = op.parse_args()

    print options.inpath_1702 
    print options.inpath_4960 
    print options.inpath_4961 
    print options.inpath_3620
    print options.outpath

    data_1702 = sc.textFile(options.inpath_1702).flatMap(lambda x:get_sequence_1702(x))

    data_4960 = sc.textFile(options.inpath_4960).flatMap(lambda x:get_sequence_4960_4961(x))

    data_4961 = sc.textFile(options.inpath_4961).flatMap(lambda x:get_sequence_4960_4961(x))
    
    data_3620 = sc.textFile(options.inpath_3620).flatMap(lambda x:get_sequence_4960_4961(x))

    datas = [data_1702, data_4960, data_4961, data_3620]
    #datas = [data_1702, data_4960]

    data = sc.union(datas)

    res = data.reduceByKey(lambda x,y:x+y).\
            map(lambda (openId, articles): merge_sequence(openId, articles)).coalesce(512).cache()
    res.saveAsTextFile(options.outpath, "org.apache.hadoop.io.compress.GzipCodec")


    length = res.map(lambda x:(len(x.split('\t')[1].split(',')), 1)).\
            persist(storageLevel=StorageLevel.MEMORY_AND_DISK_SER)

    #print "# of length == 50:", length.filter(lambda x:x[0] >= 50).count()
    print "# of length >= 20:", length.filter(lambda x:x[0] >= 20).count()

    avg = length.reduce(lambda x,y: (x[0]+y[0], x[1]+y[1]))
    print "average length:", avg[0], avg[1], float(avg[0])/avg[1]
    
    sc.stop()
