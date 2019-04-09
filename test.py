#!/usr/bin/env python
# -*- coding:utf8 -*-
import sys
import redis
'''
redis连接池
'''

pool = redis.ConnectionPool(host='127.0.0.1',port = 6379,db=0,password='12345')
redis = redis.Redis(connection_pool=pool)
redis.set("test",1)
print redis.get("test")
