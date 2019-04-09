#!/bin/bash

spark_path=/data/spark/spark-2.4/bin
hadoop_path=/data/hadoop/bin

$hadoop_path/hadoop fs -rm -r /test/out
$hadoop_path/hadoop fs -rm -r /test/wc.result
$spark_path/spark-submit  --master spark://ubuntu:7077  /data/test/sparkdemo1.py
$hadoop_path/hadoop  fs -get /test/out/part-00000 /data/temp/
$hadoop_path/hadoop  fs -rm -r /test/out
$hadoop_path/hadoop fs -put /data/temp/part-00000 /test/wc.result
rm /data/temp/part-00000

