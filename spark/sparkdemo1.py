#!/usr/bin/python
# coding=utf-8
from pyspark import SparkConf ,SparkContext
conf = SparkConf().setMaster("local").setAppName('My App')
#创建spark实例
sc= SparkContext(conf = conf)
#创建RDD
lines = sc.textFile("hdfs://ubuntu:9000/test/wc.txt")

class test :

    def f(self,x):
        if x != "Car" :
            return
        return x
#转换算子  懒加载  缓存后再次使用速度快
c = lines.flatMap(lambda x :x.split(' ')).filter(lambda x :test().f(x)).cache()
wc = c.map(lambda w :(w,1)).reduceByKey(lambda a,b :a+b)
# 转成list
# print wc.collect()

#take获取数据 take(1)  == first
for line in wc.take(int(wc.count())):
    print line

wc.saveAsHadoopFile(path="hdfs://ubuntu:9000/test/out",outputFormatClass="org.apache.hadoop.mapred.TextOutputFormat",conf={'mapreduce.output.textoutputformat.separator':','})
sc.stop()

