#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, sys, json
import redis
import time, datetime
import logging
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch import ConnectionTimeout

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%d %b %Y %H:%M:%S',
                    filename='/opt/slowlog/redis.log',
                    filemode='a')

es_target_server = "172.18.21.3"                #es节点地址
es_source = Elasticsearch([es_target_server])
index_name = 'redis_slowlog'
index_type = 'slowlog_info'


def get_hosts():
    with open('/opt/slowlog/hosts.txt') as f:
        l = f.readlines()
        l = [i.rstrip('\n') for i in l]
        return l

def slowlog(slow, host):
    values_list = []
    try:
        for i in slow[0]:
            data = {}
            data['host'] = host
            a = i['start_time'] + 28800
            data['@timestamp'] = time.strftime("%Y-%m-%dT%H:%M:%S.000+08:00", time.gmtime(a))
            data['date_ymd'] = int(time.strftime("%Y%m%d", time.gmtime(a)))
            ym = time.strftime("%Y%m", time.gmtime(a))
            data['date_hms'] = int(time.strftime("%H%M%S", time.gmtime(a)))
            b = i['duration'] / 1000
            data['duration'] = b
            c = i['command'].decode()
            data['command'] = c.split()[0]
            data['parameter'] = "".join(c.split()[1:])
            data['details'] = c
            data['role'] = slow[1]
            data['id'] = i['id']
            values_list.append({"_index": index_name + ym, "_type": index_type, "_source": data})
            #print(values_list)
        try:
            helpers.bulk(es_source, values_list)
        except Exception as err:
            logging.error(err)
    except Exception as err:
        logging.error(err)

def getslow(conn):
    try:
        slow = conn.slowlog_get(256)
        if len(slow) > 0:
            conn.slowlog_reset()
            return slow, role
        else:
            return 'no slowlog'
    except Exception as err:
        logging.error(err)
        return 'error'

def slowlog_monitor():
    values_list = []
    for host in get_hosts():
        conn = redis.StrictRedis(host=host, port=6379)
        slow = getslow(conn)
        if slow == 'error':
            msg = "host:%s connect fail." % host
            logging.error(msg)
        elif slow == 'no slowlog':
            msg = "host:%s no slowlog." % host
            logging.info(msg)
        else:
            slowlog(slow, host)

if __name__ == '__main__':
    slowlog_monitor()
