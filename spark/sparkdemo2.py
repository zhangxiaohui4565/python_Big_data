#!/usr/bin/python
# coding=utf-8

import csv

from io import StringIO
from pyspark import SparkConf ,SparkContext
conf = SparkConf().setMaster("local").setAppName('My App')
#预先加载配置
sc= SparkContext(conf = conf)
#创建RDD

# lines = sc.textFile("hdfs://10.1.1.18:9000/input/image/day/*")
lines = sc.textFile("D:\推荐系统数据/20190222")
# lines1 = sc.textFile("/data/test/20190222")
class test :

    def f(self,x):
        word = x.split(',')
        if (word[len(word)-1]) == 'commend.preview':
            return x
        return
#转换算子  懒加载  缓存后再次使用速度快
# c = lines.flatMap(lambda x :x.split(' ')).filter(lambda x :test().f(x)).cache()
# wc = c.map(lambda w :(w,1)).reduceByKey(lambda a,b :a+b)


c = lines.filter(lambda x : test().f(x))



#take获取数据 take(1)  == first
d = {}
for line in c.take(int(c.count())):

    words = line.split(',')
    row = words[3]
    if  not (row in d ):
        d [row] = 0
    d[row] += 1
for i  in  d :
    print ("%s,%s" %(i,d[i]))
rddData = sc.parallelize(d.items())
# c.saveAsHadoopFile(path="hdfs://ubuntu:9000/test/out",outputFormatClass="org.apache.hadoop.mapred.TextOutputFormat",conf={'mapreduce.output.textoutputformat.separator':','})
# print(lines1.count())
# def writeRecords(records):
#
#      output = StringIO()
#      writer = csv.DictWriter(output, fieldnames=["row", "count"])
#      for record in records:
#
#         writer.writerow(record)
#      return [output.getvalue()]
rddData.saveAsTextFile("D://testdata/out4")
sc.stop()
