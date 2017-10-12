#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import pymysql
import logging

class mysqlUri():

    def __init__(self):
        pass

    def get_datacenter_mysql_uri(self):
        # config = {'host': '127.0.0.1',
        #           'user': 'root',
        #           'password': 'root',
        #           'port': 3306,
        #           'database': 'data_center',
        #           'charset': 'utf8'
        #           }
        config = {'host': '120.26.12.152',
                  'user': 'dc_select',
                  'password': 'F8E&iXZLvG1V#qCt',
                  'port': 3306,
                  'database': 'data_center',
                  'charset': 'utf8'
                  }
        return config

    def get_ths_mysql_uri(self):
        config = {'host': '120.26.12.152',
                  'user': 'qhqm_select',
                  'password': 'j-yl"?4T}u.xj>#W',
                  'port': 3306,
                  'database': 'qhqm',
                  'charset': 'utf8'
                  }
        return config

    def start_datacenter_MySQL(self, config):
        try:
            conn = pymysql.connect(**config)
        except Exception, e:
            logging.error(e)
            return False
        conn.autocommit(1)
        cursor = conn.cursor()
        myConn_list = [conn, cursor]
        return myConn_list

    def close_datacenter_MySQL(self, conn, cursor):
        cursor.close()
        conn.commit()
        conn.close()

    def start_ths_MySQL(self, config):
        try:
            conn = pymysql.connect(**config)
        except Exception, e:
            logging.error(e)
            return False
        conn.autocommit(1)
        cursor = conn.cursor()
        myConn_list = [conn, cursor]
        return myConn_list

    def close_ths_MySQL(self, conn, cursor):
        cursor.close()
        conn.commit()
        conn.close()
