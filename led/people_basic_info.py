#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import urllib
import pymysql
import logging
import requests
from mysql import mysqlUri
from bs4 import BeautifulSoup
from ProxyDemo import proxyListGet
import threading
import Queue

import pymongo
from datetime import datetime


LOG_FILENAME = "./log.txt"

target_url = "http://vip.stock.finance.sina.com.cn/corp/go.php/vCI_CorpManager/stockid/000001.phtml"

direct_url = "http://vip.stock.finance.sina.com.cn/corp/go.php/vCI_CorpManager/stockid/%s.phtml"

name_url = 'http://vip.stock.finance.sina.com.cn/corp/view/vCI_CorpManagerInfo.php?stockid=%s&Name=%s'

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


class posSpider:

    filed_map = {
        u"姓名": "peo_name",
        u"性别": "sex_par",
        u"出生日期": "birth_day",
        u"学历": "high_edu",
        u"国籍": "country",
        u"简历": "back_gro"
    }

    def __init__(self, conn, cursor):
        self.conn = conn
        self.cursor = cursor

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
                self.codes.append(r[0])
        except Exception, e:
            logging.error(e)

    def insert_data(self, title, datas):
        values = []

        keys = "people_char, come_source"
        # keys = keys + "," + ""
        values.append(1012003)
        values.append(u'新浪财经')
        number = "%s, %s"
        for j in range(0, len(title)):
            if self.filed_map.get(title[j]):
                keys = keys + "," + self.filed_map[title[j]]
                number = number + "," + "%s"
                values.append(datas[j])
        insert_sql = "insert into people_basic_info (" + keys + ")" + " values (" + number + ")"
        try:
            self.cursor.execute(insert_sql, values)
            self.conn.commit()
        except Exception, e:
            logging.error(e)

    def write_lose(self, c):
        fp = open('lose_people.txt', 'a')
        fp.writelines(c + ' stock ' + '\n')
        fp.close()

    def write_lose_name(self, c, name):
        fp = open('lose_people.txt', 'a')
        fp.writelines(c + name + '\n')
        fp.close()

    def work(self, c, html):
        global new_num

        try:
            soup = BeautifulSoup(html.content.decode('gbk'), "lxml")
        except Exception, e:
            self.write_lose(c)
            return

        tbs = soup.find("div", attrs={"class": "R"}).find_all('table', id='comInfo1')
        if len(tbs) == 0:
            self.write_lose()
            return
        names = []  # 用于防止重名
        for i in range(0, len(tbs)):
            try:
                trs = tbs[i].find('tbody').find_all('tr', recursive=False)
            except Exception, e:
                self.write_lose(c)
                return
            if i == 0:
                start = 1
            else:
                start = 2
            for j in range(start, len(trs)):

                title = []  # 表头
                datas = []  # 数据

                tdsss = trs[j].find_all('td', recursive=False)
                if len(tdsss) < 4:
                    break
                name = trs[j].find('td').text.replace(' ', '').replace('\n', '').replace('\t', '')
                if name not in names:  # 该名字从未出现过
                    new_num += 1
                    names.append(name)
                    print new_num, name
                    s = urllib.quote(name.decode(sys.stdin.encoding).encode('gbk'))

                    r = ''
                    flag_r = 0
                    for count in range(0, 11):
                        try:
                            r = requests.get(name_url % (c, s))
                            break
                        except Exception, e:
                            if count == 10:
                                self.write_lose_name(c, name)
                                flag_r = 1
                                break
                            else:
                                continue
                    if flag_r == 1:
                        continue
                    try:
                        rr = BeautifulSoup(r.content.decode('gbk'), "lxml")
                        rrr = rr.find('table', id='Table1')
                    except Exception, e:
                        self.write_lose_name(c, name)
                        continue
                    ths = rrr.find('thead').find('tr').find_all('th', recursive=False)
                    flag = 0  # 用于获取学历下标
                    flag_sex = 0  # 用于获取性别下标
                    flag_c = 0  # 用于获取国籍下标
                    for k in range(0, len(ths)):
                        text = ths[k].text.replace(' ', '').replace('\n', '').replace('\t', '')
                        title.append(text)
                        if text == u'学历':
                            flag = k
                        elif text == u'性别':
                            flag_sex = k
                        elif text == u'国籍':
                            flag_c = k
                    tds = rrr.find('tbody').find('tr').find_all('td', recursive=False)
                    for k in range(0, len(tds)):
                        text = tds[k].text.replace(' ', '').replace('\n', '').replace('\t', '')
                        if k == flag:  # 如果是学历
                            if text == u'研究生' or text == u'硕士研究生' or text == u'硕士(MBA)':
                                text = u'硕士'
                            zhiwei_sql = 'select system_uni_code from system_const where system_name = %s'
                            self.cursor.execute(zhiwei_sql, (text))
                            res = self.cursor.fetchall()
                            if len(res) == 0:  # 不存在该学历对应代码
                                text = 1010000
                            else:
                                text = res[0][0]
                        elif k == flag_sex:  # 如果是性别
                            if text != u'男' and text != u'女':
                                text = '未知'
                        elif k == flag_c:  # 如果是国籍
                            if text == u'中国':
                                text = 100000
                            else:
                                text = 0
                        datas.append(text)

                    trss = rrr.find('tbody').find_all('tr')
                    if len(trss) >= 2:
                        title.append(trss[1].find_all('td', recursive=False)[0].text.replace(' ', '').replace('\n', '').replace('\t', ''))
                        datas.append(trss[1].find_all('td', recursive=False)[1].text.replace(' ', '').replace('\n', '').replace('\t', ''))

                    self.insert_data(title, datas)


class myThread(threading.Thread):

    def __init__(self, q):
        global Thread_id
        threading.Thread.__init__(self)
        self.q = q
        self.Thread_id = Thread_id
        Thread_id = Thread_id + 1

    def run(self):
        while True:
            try:
                task = self.q.get(block=True, timeout=1)  # 不设置阻塞的话会一直去尝试获取资源
            except Queue.Empty:
                print 'Thread',  self.Thread_id, 'end'
                break
            # 取到数据，开始处理（依据需求加处理代码）
            # print "Starting ", self.Thread_id
            # print task
            self.q.task_done()


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

    spider = posSpider(conn, cursor)
    spider.get_codes()

    for c in spider.codes:
        html = pro_use.use_proxy(direct_url % (c))
        print c
        spider.work(c=c, html=html)

