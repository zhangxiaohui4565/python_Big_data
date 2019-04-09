#!/usr/bin/python
# coding=utf-8
from pyspark import SparkConf ,SparkContext
conf = SparkConf().setMaster("local").setAppName('My App')
sc= SparkContext(conf = conf)
#创建RDD
lines1 = sc.parallelize() #将集合转化成RDD
lines = sc.textFile("/data/test/20190222")
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
    if  not (d.has_key(row)):
        d [row] = 0
    d[row] += 1
for i  in d :
    print "%s,%s" %(i,d[i])
# c.saveAsHadoopFile(path="hdfs://ubuntu:9000/test/out",outputFormatClass="org.apache.hadoop.mapred.TextOutputFormat",conf={'mapreduce.output.textoutputformat.separator':','})
sc.stop()
