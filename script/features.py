#!/usr/bin/python
#coding=utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from functools import partial
from operator import add
import json
import sys
import random
import mmh3

def extract_features(line):
    reload(sys) 
    sys.setdefaultencoding('utf-8')
    rid,pos,item_feat,user_feat,action_type,dt = line

    fList = []
    ifeatList = json.loads(item_feat)
    for feaInfo in ifeatList:
        key = feaInfo['key']
        if key.startswith("CITY#"):
            if key.endswith("CTR") or key.endswith("CVR"):
                continue
        idx = feaInfo['id']
        val = feaInfo['value']
        fList.append(str(idx)+":"+str(val))
    ufeatList = json.loads(user_feat)
    for feaInfo in ufeatList:
        key = feaInfo['key']
        if key.startswith("PRICE#") or key.startswith("SHORT#") or key.startswith("LONG#"):
            continue
        idx = feaInfo['id']
        val = feaInfo['value']
        fList.append(str(idx)+":"+str(val))
        
    if pos != None:
        key = "POS#"+pos
        idx = mmh3.hash(key,12345,signed=False) % 1000000000
        fList.append(str(idx)+":1.0")
    fStr = " ".join(fList)

    label = "0"
    times = 1
    if action_type in ("2","21"):
        label = "1"
    if action_type in ("3","7","18"):
        label = "1"
        times = 1
    sampleStr = label + " " + fStr

    sample_list = []
    for ti in xrange(times):
        sample_list.append(sampleStr)
    return sample_list

def main():
    dt = sys.argv[1]

    spark = SparkSession.builder\
        .master("yarn")\
        .appName("list_features")\
        .enableHiveSupport()\
        .getOrCreate()

    rawSql = "select request_id,pos,item_feat,user_feat,action_type,date from test.list_daily where date = '%s'"%(dt)
    samples = spark.sql(rawSql).rdd.flatMap(extract_features).saveAsTextFile("/user/train")
    #samples_schema = StructType([
    #    StructField("sample", StringType(), True)
    #])
    #samples_df = spark.createDataFrame(samples, samples_schema)
    #samples_table = "test.tmp_test"
    #samples_df.write.saveAsTable(samples_table, format = 'orc', mode = 'overwrite')

    spark.stop()

if __name__ == '__main__':
    main()
