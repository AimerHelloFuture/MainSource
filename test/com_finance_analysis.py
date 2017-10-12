#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup

# datas = {
#             'button2' : '%CC%E1%BD%BB',
#             'cwzb' : 'financialreport',
#             'mm' : '-03-31',
#             'yyyy' : '2016'
#         }
# header = {
#             'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
#             'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
#             'Accept-Language': 'zh-CN,zh;q=0.8',
#             'Accept-Encoding': 'gzip, deflate',
#             'Connection': 'keep-alive',
#             'Origin': 'http://www.cninfo.com.cn',
#             'Upgrade-Insecure-Requests': '1',
#             'Host' : 'www.cninfo.com.cn',
#             # 'Referer' : 'http://www.cninfo.com.cn/information/stock/financialreport_.jsp?stockCode=000001'
#          }
#
# session = requests.session()
# m = session.get(url='http://www.cninfo.com.cn/cninfo-new/index', headers=header)
# r = session.post(url='http://www.cninfo.com.cn/information/stock/financialreport_.jsp?stockCode=000001&key=0.7667284427047751', data = datas, headers=header)
# # req = session.get('http://www.cninfo.com.cn/information/stock/financialreport_.jsp?stockCode=000001', headers = header)
#
#
# # r = requests.post('http://www.cninfo.com.cn/information/stock/financialreport_.jsp?stockCode=000001', data = datas)
# # # req = requests.get('http://www.cninfo.com.cn/information/stock/financialreport_.jsp?stockCode=000001', headers = header)
# # print r.status_code
# print r.text
#
# # soup = BeautifulSoup(r.text, 'lxml')
# #
# # print soup
# # print ''



spider_url = "http://www.cninfo.com.cn/information/stock/financialreport_.jsp?stockCode=%s"
redirect_url = "http://www.cninfo.com.cn"

yyyy = "2017"
mm = "-03-31"
cwzb = "financialreport"
button2 = "%CC%E1%BD%BB"
post_data = {
    "yyyy": yyyy,
    "mm": mm,
    "cwzb": cwzb,
    "button2": button2,
}

code = 0000001

r = requests.session()

r.headers.update({"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36"})

r.headers.update({"Referer": "http://www.cninfo.com.cn/information/financialreport/szmb%s.html" % code})

req = r.post(spider_url % code, data=post_data)

soup = BeautifulSoup(req.text, "lxml")

print soup