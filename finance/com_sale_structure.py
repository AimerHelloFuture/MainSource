#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import sys
import random
import pymongo
import time
import Queue
import threading
from bs4 import BeautifulSoup
from ProxyDemo import proxyListGet
import logging
import requests
from mysql import mysqlUri
from datetime import datetime

LOG_FILENAME = "./log_sale_structure.txt"

target_url = "http://stockpage.10jqka.com.cn"

direct_url = "http://stockpage.10jqka.com.cn/%s/operate/"

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11'
    }

new_num = 0

class proxyUse():
    def __init__(self, proxieslist):
        self.proxieslist = proxieslist

    def use_proxy(self, url):
        for ii in self.proxieslist:
            try:
                # html = requests.get(url, proxies=ii)
                html = requests.get(url, headers=headers, proxies=ii)
                html.raise_for_status()
                # tml = requests.get(url, headers=headers)
                return html
            except Exception, e:
                continue


class managerSpider():

    redirect_url = "http://emweb.securities.eastmoney.com/PC_HSF10/BusinessAnalysis/BusinessAnalysisAjax?code=%s"

    sale_map = {
        u"项目类型": "project_type",
        u"业务名称": "project_name",
        u"营业收入(元)": "income",
        u"收入比例": "income_rate",
        u"营业成本(元)": "cost",
        u"成本比例": "cost_rate",
        u"利润比例": "gros_prof_marg",
        u"毛利率": "gros_prof_rate"
    }

    def __init__(self, conn, cursor, period):
        self.conn = conn
        self.cursor = cursor

        self._sess = requests.session()
        self._sess.headers.update({"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36"})

        self.account_date = period + " 00:00:00"
        self.period = period

        self.period_num = 1

        if "03-31" in self.period:
            self.period_num = 1
            self.report_type = 1015001
        elif "06-30" in self.period:
            self.period_num = 2
            self.report_type = 1015002
        elif "09-30" in self.period:
            self.period_num = 3
            self.report_type = 1015003
        elif "12-31" in self.period:
            self.period_num = 4
            self.report_type = 1015004

        self.codes = []

    def __del__(self):
        self._sess.close()
        self.cursor.close()
        self.conn.commit()
        self.conn.close()

    def get_all_codes(self):
        select_sql = "select sec_code from sec_basic_info"
        try:
            cur = self.cursor
            cur.execute(select_sql)
            res = cur.fetchall()
            # res = cur.fetchone()
            for r in res:
                if r[0] != u'':
                    self.codes.append(r[0])
        except Exception, e:
            logging.error(e)

    def insert_data(self, c, title, datas, com_uni_code):

        global new_num

        for i in range(0, len(datas), len(title)):

            keys = "com_uni_code,end_date,reporttype"
            number = "%s,%s,%s"

            values = [com_uni_code, self.account_date, self.report_type]

            for j in range(0, len(title)):
                if self.sale_map.get(title[j]):
                    keys = keys + "," + self.sale_map[title[j]]
                    number = number + "," + "%s"
                    # remark = remark + self.normal_map[r] +','
                    values.append(datas[i + j])

            insert_sql = "insert into com_sale_structure" + " (" + keys + ")" + " values (" + number + ")"
            try:
                self.cursor.execute(insert_sql, values)
                new_num += 1
                self.conn.commit()
            except Exception, e:
                logging.error(e)
                continue

            print new_num, c, com_uni_code, datas[i], datas[i+1]

    def find_project_type(self, name):
        if name == u'按行业':
            return 1517001
        elif name == u'按产品':
            return 1517002
        elif name == u'按地区':
            return 1517003
        else:
            return 1517000

    def to_float(self, text):
        if u'%' in text:
            return float(text.replace(u'%', ''))
        elif u'亿' in text:
            return float(text.replace(u'亿', '')) * 100000000.0
        elif u'万' in text:
            return float(text.replace(u'万', '')) * 10000.0
        elif text == u'-':
            return None
        else:
            try:
                return float(text)
            except Exception, e:
                logging.error(e)
                print text
                return text

    def work(self, c, html, com_uni_code):

        soup = BeautifulSoup(html.text, 'lxml')

        try:
            zygcfx = soup.find('div', id='analysis').find('div', class_='bd pt5').find('div', class_='m_tab mt15').find('li', class_='cur')
        except Exception, e:
            zygcfx = None
        if not zygcfx:
            return

        if self.period != zygcfx.text.strip():
            return

        try:
            table = soup.find('div', id='analysis').find('div', class_='bd pt5').find('div', class_='m_tab_content').find('table')
        except Exception, e:
            table = None
        if not table:
            return

        title = []
        datas = []

        ths = table.find('thead').find('tr').find_all('th', recursive=False)
        title.append(u'项目类型')
        for i in range(0, len(ths)):
            title.append(ths[i].text.strip())

        trs = table.find('tbody').find_all('tr', recursive=False)

        dijihang = 0
        type = 0
        while dijihang < len(trs):
            rowspan = int(trs[dijihang].find('th').get('rowspan'))
            for i in range(dijihang, dijihang + rowspan):
                tds = trs[i].find_all('td', recursive=False)
                if i == dijihang:
                    type = self.find_project_type(trs[dijihang].find('th').text.strip().replace('\n', ''))
                    datas.append(type)
                    datas.append(tds[0].text.strip())
                    for j in range(1, len(tds)):
                        datas.append(self.to_float(tds[j].text.strip()))
                else:
                    datas.append(type)
                    datas.append(tds[0].text.strip())
                    for j in range(1, len(tds)):
                        datas.append(self.to_float(tds[j].text.strip()))
            dijihang = dijihang + rowspan

        self.insert_data(c, title, datas, com_uni_code)


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S', filename=LOG_FILENAME, filemode='w')

    '''
    获取股票可用代理list
    '''
    pro_list = proxyListGet()
    while True:
        try:
            proxieslists = pro_list.GetProxyList(testurl=target_url)
            if len(proxieslists) == 0:
                continue
            break
        except Exception, e:
            continue
    pro_use = proxyUse(proxieslists)

    uri = mysqlUri()
    config = uri.get_mysql_uri()
    myConn_list = uri.start_MySQL(config)
    conn = myConn_list[0]
    cursor = myConn_list[1]

    spider = managerSpider(conn, cursor, '2017-06-30')
    spider.get_all_codes()
    com_uni_codes = []
    for c in spider.codes:

        select_sql1 = "select com_uni_code from sec_basic_info where sec_code = %s"
        cursor.execute(select_sql1, c)
        res = cursor.fetchall()
        com_uni_code = res[0][0]
        if com_uni_code not in com_uni_codes:
            com_uni_codes.append(com_uni_code)
            temp_url = direct_url % (c)
            html = pro_use.use_proxy(temp_url)
            spider.work(c=c, html=html, com_uni_code=com_uni_code)
