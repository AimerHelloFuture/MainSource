#!/usr/bin/python
# -*- coding: utf-8 -*-

import pymysql


class MyPymysql(object):
    def __init__(self):
        """
        初始化MySQL连接
        """
        self.__connection = None

    def __del__(self):
        """
        如果未关闭MySQL连接，关闭连接
        :return: 无
        """
        try:
            self.__connection.close()
        except Exception:
            pass

    def connect(self, connect_type="localhost"):
        """
        连接MySQL数据库
        :param connect_type: online或者其他
        :return: 无
        """
        if connect_type == "qhqm_select":
            host = '120.26.12.152'
            port = 3306
            user = 'qhqm_select'
            password = 'j-yl"?4T}u.xj>#W'
            db = 'qhqm'
            charset = 'utf8'
        elif connect_type == "qhqm":
            host = '10.252.218.51'
            port = 3306
            user = 'qhqm'
            password = 'ewB}1H^]|eXm52$T'
            db = 'qhqm'
            charset = 'utf8'
        elif connect_type == "dc_select":
            host = '120.26.12.152'
            port = 3306
            user = 'dc_select'
            password = 'F8E&iXZLvG1V#qCt'
            db = 'data_center'
            charset = 'utf8'
        elif connect_type == "hs_dev":
            host = '10.252.218.51'
            port = 3306
            user = 'hs_dev'
            password = 'Gz22m$s86ff[V?V5'
            db = 'data_center'
            charset = 'utf8'
        elif connect_type == "dc_local":
            host = '10.11.255.60'
            port = 3306
            user = 'data_center'
            password = 'Wox4I*2pXe#l'
            db = 'data_center'
            charset = 'utf8'
        else:
            host = 'localhost'
            port = 3306
            user = 'root'
            password = 'root'
            db = 'data_center'
            charset = 'utf8mb4'
        while True:
            try:
                self.__connection = pymysql.connect(host=host, port=port, user=user, password=password,
                                                    db=db, charset=charset, cursorclass=pymysql.cursors.DictCursor)
            except Exception:
                continue
            break

    def run(self, query, args=None):
        """
        执行MySQL语句
        :param query: 语句
        :param args: 参数，默认None
        :return: 执行结果
        """
        if self.__connection is None:
            self.connect()
        with self.__connection.cursor() as cursor:
            cursor.execute(query, args)
            result = cursor.fetchall()
        return result

    def commit(self):
        """
        提交MySQL修改
        :return: 无
        """
        self.__connection.commit()

    def close(self):
        """
        关闭MySQL连接
        :return: 无
        """
        self.__connection.close()
