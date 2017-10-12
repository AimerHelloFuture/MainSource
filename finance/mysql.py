#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import env
import pymysql
import logging

class mysqlUri():

    def __init__(self):
        pass

    def get_mysql_uri(self):
        if env.ENV == 'online':
            config = {'host': '10.11.255.60',
                      'user': 'data_center',
                      'password': 'Wox4I*2pXe#l',
                      'port': 3306,
                      'database': 'data_center',
                      'charset': 'utf8'
                      }
            return config
        else:
            config = {'host': '127.0.0.1',
                      'user': 'root',
                      'password': '',
                      'port': 3306,
                      'database': 'data_center',
                      'charset': 'utf8'
                      }
            return config

    def start_MySQL(self, config):
        try:
            conn = pymysql.connect(**config)
        except Exception, e:
            logging.error(e)
            return False
        conn.autocommit(1)
        cursor = conn.cursor()
        myConn_list = [conn, cursor]
        return myConn_list

    def close_MySQL(self, conn, cursor):
        cursor.close()
        conn.commit()
        conn.close()
