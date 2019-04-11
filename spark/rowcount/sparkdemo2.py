#!/usr/bin/python
# coding=utf-8

import os,shutil

from io import StringIO
from pyspark import SparkConf ,SparkContext
conf = SparkConf()
#预先加载配置
sc= SparkContext(conf = conf)
#创建RDD
lines = sc.textFile("hdfs://ubuntu:9000/input/image/day/*")
class test :

    def f(self,x):
        word = x.split(',')
        if (word[len(word)-1]) == 'commend.preview':
            return x
        return
    def toStr(self,x):
        if int(x) < 10 :
            x = '0'+ x
        return x

c = lines.filter(lambda x : test().f(x)).map(lambda x:x.split(',')[3]).map(lambda x : test().toStr(x)).map(lambda x :(x,1)).reduceByKey(lambda x,y :x+y).sortByKey()
c.repartition(1).saveAsHadoopFile(path="hdfs://ubuntu:9000/test/out",outputFormatClass="org.apache.hadoop.mapred.TextOutputFormat",conf={'mapreduce.output.textoutputformat.separator':','})
sc.stop()
# path = "D://testdata/out"
# print(c.collect())
# if os.path.exists(path):
#     shutil.rmtree(path)
# c.saveAsTextFile(path)
# sc.stop()
