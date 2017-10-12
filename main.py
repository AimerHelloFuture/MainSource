#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import env
import pymysql
import logging
from mysql import mysqlUri

LOG_FILENAME = "./log.txt"
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S', filename=LOG_FILENAME, filemode='w')

url = mysqlUri()
config = url.get_mysql_uri()
myConn_list = url.start_MySQL(config)

