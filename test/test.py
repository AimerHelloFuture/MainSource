# coding=utf-8

import requests
from bs4 import BeautifulSoup
import pymysql
import logging
import time
import re

# str =   '''
#       	 location.href ='../../information/stock/financialreport_.jsp?stockCode=600367';
#         '''
#
# str = str.split("location.href =")[1].split('\'../..')[1].split('\';')[0]
# print str
# print str[1]
#
# data = '2017-03-31 00:00:00'
# print data[4:9]
#
# re.match(r'[+-]?d+$', '-1234')
#
# import unicodedata
# print unicodedata.numeric(u"2")
# print float('四')


def work():
    m = 3
    m = 5
    if 1:
        m = 4
        # print m
    else:
        m = 5
        # print m
    print m

# m = 3
# if 1:
#     m = 4
#     # print m
# else:
#     m = 5
#     # print m
#
# mm = 2


work()