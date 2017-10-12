#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import pymysql
import logging
import requests
from mysql import mysqlUri
from bs4 import BeautifulSoup
from ProxyDemo import proxyListGet
import pymongo
from datetime import datetime

LOG_FILENAME = "./log_com_equity_change.txt"

target_url = "http://finance.sina.com.cn/"

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11'
    }

new_num = 0

end_date = '2017-06-30'   # 即要爬取的报告期


class proxyUse():
    def __init__(self, url, proxieslist):
        self.url = url
        self.proxieslist = proxieslist

    def use_proxy(self):
        for ii in self.proxieslist:
            try:
                # html = requests.get(url, proxies=ii)
                html = requests.get(self.url, headers=headers, proxies=ii)
                html.raise_for_status()
                # tml = requests.get(url, headers=headers)
                return ii
            except Exception, e:
                continue


class balanceSpider:

    spider_url = "http://money.finance.sina.com.cn/corp/go.php/vFD_BenifitChange/stockid/%s/displaytype/1000.phtml"

    account_date = "2017-03-31 00:00:00"

    def __init__(self, conn, cursor, period):
        self.conn = conn
        self.cursor = cursor

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
        self.cursor.close()
        self.conn.commit()
        self.conn.close()

    def get_codes(self):
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

    def insert_data(self, datas, com_uni_code, c):

        global new_num
        keys = "com_uni_code, end_date, item, begin_num, this_period_add, this_period_reduce, end_num, change_reason"
        number = "%s, %s, %s, %s, %s, %s, %s, %s"

        for i in range(0, len(datas), 7):
            values = [com_uni_code, datas[i], datas[i+1], datas[i+2], datas[i+3], datas[i+4], datas[i+5], datas[i+6]]
            insert_sql = "insert into com_equity_change" + " (" + keys + ")" + " values (" + number + ")"
            try:
                self.cursor.execute(insert_sql, values)
                new_num += 1
                self.conn.commit()
                print new_num, c, com_uni_code, datas[i], datas[i+1]
            except Exception, e:
                logging.error(e)

    def tofloat(self, fdata): # 287,670,026.58 -- > 287670026.58

        if u'元' in fdata and u'千' not in fdata and u'万' not in fdata:
            flag = 0
            fdata = fdata.replace(u'元', '')
        elif u'千元' in fdata:
            flag = 1
            fdata = fdata.replace(u'千元', '')
        elif u'万元' in fdata:
            flag = 2
            fdata = fdata.replace(u'万元', '')
        elif u'百万' in fdata:
            flag = 3
            fdata = fdata.replace(u'百万', '')
        else:
            print fdata
            m = fdata.index(u'千元')
            fp = open('lose_equity_danwei_sina.txt', 'a')
            fp.writelines(self.account_date + ' ' + c + ' ' + fdata + '\n')
            fp.close()
            return

        if fdata[0] != "-":
            fs = fdata.split(",")
            flen = len(fs)
            result = 0
            for f in fs:
                result = result + float(f)
                if flen == 1:
                    if flag == 0:
                        return result
                    elif flag == 1:
                        return result * 1000.0
                    elif flag == 2:
                        return result * 10000.0
                    elif flag == 3:
                        return result * 1000000.0
                else:
                    result = result * 1000.0
                    flen = flen - 1
        else:
            fs = fdata.split(",")
            fs[0] = fs[0][1:]
            flen = len(fs)
            result = 0
            for f in fs:
                result = result + float(f)
                if flen == 1:
                    if flag == 0:
                        return result * -1.0
                    elif flag == 1:
                        return result * -1.0 * 1000.0
                    elif flag == 2:
                        return result * -1.0 * 10000.0
                    elif flag == 3:
                        return result * -1.0 * 1000000.0
                else:
                    result = result * 1000.0
                    flen = flen - 1

    def work(self, c, proxy, com_uni_code):

        table_name = 'com_equity_change'
        come_source_url = self.spider_url % (c)
        # come_source = u'新浪财经' + come_source_url

        # 增量部分：若数据库中已有记录则无需再次插入
        select_sql = 'select * from ' + table_name + ' where com_uni_code = %s and end_date = %s'
        try:
            self.cursor.execute(select_sql, (com_uni_code, self.account_date))
            self.conn.commit()
        except Exception, e:
            logging.error(e)
        res_s = self.cursor.fetchall()
        if len(res_s) > 0:
            return

        post_count = 1
        while True:
            try:
                r = requests.get(come_source_url, headers=headers, proxies=proxy, timeout=10)
                break
            except Exception, e:
                post_count += 1
                if post_count > 5:
                    fp = open('lose_com_equity_change.txt', 'a')
                    fp.writelines(self.account_date + c + ' get error' + '\n')
                    fp.close()
                    return
                continue

        if self.period not in r.text:
            # fp = open('lose_balance_sina.txt', 'a')
            # fp.writelines(self.account_date + code + self.period + ' get error' + '\n')
            # fp.close()
            return

        soup = BeautifulSoup(r.content.decode('gbk'), "lxml")

        try:
            trs = soup.find('table', id='BalanceSheetNewTable0').find('tbody').find_all('tr', recursive=False)
        except Exception, e:
            trs = None
        if not trs:
            return

        datas = []
        line = 0
        for tr in trs:
            if line == 0:  # 忽略第一行
                line = 1
                continue
            tds = tr.find_all('td', recursive=False)
            if tds == None:
                continue
            if len(tds) == 2:
                for td in tds:
                    datas.append(td.text.strip().replace('\n', ''))

        for i in range(0, len(datas), 2):
            result[datas[i].strip()] = round(self.tofloat(datas[i + 1]), 2)*10000.0 if datas[i+1] != '--' else None

        if fin_repo_type == '':
            result = {}
        self.insert_data(result, fin_repo_type, come_source_url)
        print code, com_uni_code, balance_num, fin_repo_type, normal_num, bank_num, secrity_num, insurance_num

    def work_all(self, c, proxy, com_uni_code):

        come_source_url = self.spider_url % (c)

        post_count = 1
        while True:
            try:
                r = requests.get(come_source_url, headers=headers, proxies=proxy, timeout=10)
                break
            except Exception, e:
                post_count += 1
                if post_count > 5:
                    fp = open('lose_com_equity_change.txt', 'a')
                    fp.writelines(self.account_date + c + ' get error' + '\n')
                    fp.close()
                    return
                continue

        soup = BeautifulSoup(r.content.decode('gbk'), "lxml")

        try:
            tables = soup.find('div', class_='tagmain').find_all('table', recursive=False)
        except Exception, e:
            tables = None
        if not tables:
            return

        datas = []

        for i in range(0, len(tables) - 1):
            trs = tables[i].find('tbody').find_all('tr', recursive=False)
            if len(trs) != 32:
                fp = open('lose_com_equity_change.txt', 'a')
                fp.writelines(self.account_date + c + ' trssssss error' + '\n')
                fp.close()
                return
            else:
                riqis = trs[0].find_all('td', recursive=False)
                num = len(riqis) - 1
                for j in range(1, num+1):

                    riqi = riqis[j].text.strip() + ' 00:00:00'

                    datas.append(riqi)
                    datas.append(1)

                    for k in range(2, 7):
                        text = (trs[k].find_all('td', recursive=False))[j].text.strip()
                        data = self.tofloat(text) if text != '--' else None
                        datas.append(data)

                    datas.append(riqi)
                    datas.append(2)

                    for k in range(7, 12):
                        text = (trs[k].find_all('td', recursive=False))[j].text.strip()
                        data = self.tofloat(text) if text != '--' else None
                        datas.append(data)

                    datas.append(riqi)
                    datas.append(3)

                    for k in range(12, 17):
                        text = (trs[k].find_all('td', recursive=False))[j].text.strip()
                        data = self.tofloat(text) if text != '--' else None
                        datas.append(data)

                    datas.append(riqi)
                    datas.append(4)

                    for k in range(17, 22):
                        text = (trs[k].find_all('td', recursive=False))[j].text.strip()
                        data = self.tofloat(text) if text != '--' else None
                        datas.append(data)

                    datas.append(riqi)
                    datas.append(5)

                    for k in range(22, 27):
                        text = (trs[k].find_all('td', recursive=False))[j].text.strip()
                        data = self.tofloat(text) if text != '--' else None
                        datas.append(data)

                    datas.append(riqi)
                    datas.append(6)

                    for k in range(27, 31):
                        text = (trs[k].find_all('td', recursive=False))[j].text.strip()
                        data = self.tofloat(text) if text != '--' else None
                        datas.append(data)
                    datas.append(None)

        self.insert_data(datas, com_uni_code, c)


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
    pro_use = proxyUse(target_url, proxieslists)

    uri = mysqlUri()
    config = uri.get_mysql_uri()
    myConn_list = uri.start_MySQL(config)
    conn = myConn_list[0]
    cursor = myConn_list[1]

    spider = balanceSpider(conn, cursor, end_date)
    spider.get_codes()
    com_uni_codes = []
    for c in spider.codes[49:]:
        select_sql1 = "select com_uni_code from sec_basic_info where sec_code = %s"
        cursor.execute(select_sql1, c)
        res = cursor.fetchall()
        com_uni_code = res[0][0]
        if com_uni_code not in com_uni_codes:
            com_uni_codes.append(com_uni_code)

            proxy = pro_use.use_proxy()
            # print proxy
            spider.work_all(c, proxy, com_uni_code)
            # print c
