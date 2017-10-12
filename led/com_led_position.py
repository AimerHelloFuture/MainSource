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
delete_num = 0

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
                list = [html, ii]
                return list
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

    def insert_data(self, datas):

        keys = "com_uni_code, peo_uni_code, led_name, Post_code, in_date, off_date, come_source, post_type, post_name"
        number = "%s, %s, %s, %s, %s, %s, %s, %s, %s"
        values = datas
        # keys = keys + "," + ""
        insert_sql = "insert into com_led_position (" + keys + ")" + " values (" + number + ")"
        try:
            self.cursor.execute(insert_sql, values)
            self.conn.commit()
        except Exception, e:
            logging.error(e)

    def write_lose(self, c):
        fp = open('lose_led_position.txt', 'a')
        fp.writelines(c + ' stock ' + '\n')
        fp.close()

    def write_lose_name(self, c, name):
        fp = open('lose_led_position.txt', 'a')
        fp.writelines(c + name + '\n')
        fp.close()

    def write_lose_pos(self, c, name, pos):
        fp = open('lose_led_position.txt', 'a')
        fp.writelines(c + name + pos + '\n')
        fp.close()

    def write_repeat_name(self, c, name):
        fp = open('led_position_repeat_name.txt', 'a')
        fp.writelines(c + name + '\n')
        fp.close()

    def write_lose_pos_data(self, c, name, pos):
        fp = open('lose_led_position_data.txt', 'a')
        fp.writelines(c + name + pos + '\n')
        fp.close()

    def compare_uni(self, code, pos, name, birth, country, country_code, jianli):  # 用于查找peo_uni_code，找不到返回false，找到返回该code
        ser_sql = 'select peo_uni_code from people_basic_info where peo_name = %s and birth_day = %s'
        try:
            self.cursor.execute(ser_sql, (name, birth))
            res = self.cursor.fetchall()
            if len(res) < 1:
                self.write_lose_pos(code, name, pos)
                return False
            elif len(res) > 1:
                ser_sql2 = 'select peo_uni_code from people_basic_info where peo_name = %s and birth_day = %s and back_gro = %s'
                self.cursor.execute(ser_sql2, (name, birth, jianli))
                res2 = self.cursor.fetchall()
                if len(res2) < 1:
                    self.write_lose_pos(code, name, pos)
                    return False
                else:
                    peo_uni_code = res2[0][0]
                    if len(res2) > 1:  # 出现重复，删除人物信息表中重复数据
                        for i in range(1, len(res2)):
                            global delete_num
                            delete_num += 1
                            peo_repe_code = res2[i][0]
                            delete_sql = 'delete from people_basic_info where peo_uni_code = %s'
                            self.cursor.execute(delete_sql, (peo_repe_code))
                            self.conn.commit()
            else:
                peo_uni_code = res[0][0]
            update_remark = 'update people_basic_info set remark = %s where peo_uni_code = %s'
            if country == '':
                remark = '无'
            else:
                remark = country
            self.cursor.execute(update_remark, (remark, peo_uni_code))
            self.conn.commit()
            return peo_uni_code
        except Exception, e:
            logging.error(e)

    def date2stamp(self, date):
        try:
            return_date = datetime.strptime(date + " 00:00:00", '%Y-%m-%d %H:%M:%S')
        except Exception, e:
            return_date = None
        return return_date

    def work(self, c, html, proxy):
        global new_num

        try:
            soup = BeautifulSoup(html.content.decode('gbk'), "lxml")
        except Exception, e:
            self.write_lose(c)
            return

        tbs = soup.find("div", attrs={"class": "R"}).find_all('table', id='comInfo1')
        if len(tbs) == 0:
            self.write_lose(c)
            return
        com_source = '新浪财经'
        select_sql1 = "select com_uni_code from sec_basic_info where sec_code = %s"
        self.cursor.execute(select_sql1, c)
        res = self.cursor.fetchall()
        com_uni_code = res[0][0]

        names = []  # 用于重名，不需再次判断，直接得到uni_code
        uni_codes = []
        for i in range(0, len(tbs)):
            try:
                trs = tbs[i].find('tbody').find_all('tr', recursive=False)
            except Exception, e:
                self.write_lose(c)
                return
            if i == 0:
                start = 1
                post_type = 3
            else:
                start = 2
                if i == 1:
                    post_type = 1
                else:
                    post_type = 2
            for j in range(start, len(trs)):

                title = []  # 表头
                datas = []  # 数据

                tdsss = trs[j].find_all('td', recursive=False)
                if len(tdsss) < 4:
                    break
                name = tdsss[0].text.strip().replace('\n', '').replace('\t', '')
                pos = tdsss[1].text.strip().replace('\n', '').replace('\t', '')

                pos_sql = 'select system_uni_code from system_const where system_name = %s'
                self.cursor.execute(pos_sql, (pos))
                res = self.cursor.fetchall()
                if len(res) == 0:
                    remark = pos + '该职务代码未找到'
                    pos_code = 0
                else:
                    pos_code = res[0][0]
                    remark = ''

                start_date = tdsss[2].text.strip().replace('\n', '').replace('\t', '')
                end_date = tdsss[3].text.strip().replace('\n', '').replace('\t', '')
                new_num += 1
                print new_num, name, pos, delete_num
                if name not in names:  # 该名字从未出现过
                    names.append(name)
                    # print new_num
                    s = urllib.quote(name.decode(sys.stdin.encoding).encode('gbk'))

                    r = ''
                    flag_r = 0
                    for count in range(0, 11):
                        try:
                            r = requests.get(name_url % (c, s), headers=headers, proxies=proxy)
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

                    tds = rrr.find('tbody').find('tr').find_all('td', recursive=False)

                    birth = tds[2].text.replace(' ', '').replace('\n', '').replace('\t', '')
                    country = tds[4].text.replace(' ', '').replace('\n', '').replace('\t', '')
                    jianli = rrr.find('tbody').find_all('tr')[1].find_all('td', recursive=False)[1].text.replace(' ', '').replace('\n', '').replace('\t', '')

                    if country == u'中国':
                        country_code = 100000
                    else:
                        country_code = 0
                    peo_uni_code = self.compare_uni(c, pos, name, birth, country, country_code, jianli)
                    if peo_uni_code == False:
                        self.write_lose_pos(c, name, pos)
                        continue
                    else:
                        uni_codes.append(peo_uni_code)

                start_date = self.date2stamp(start_date)
                end_date = self.date2stamp(end_date)

                try:
                    peo_code = uni_codes[names.index(name)]
                except Exception, e:
                    self.write_lose_pos_data(c, name, pos)
                    continue

                datas.append(com_uni_code)
                datas.append(peo_code)
                datas.append(name)
                datas.append(pos_code)
                datas.append(start_date)
                datas.append(end_date)
                # datas.append(remark)
                datas.append(com_source)
                datas.append(post_type)
                datas.append(pos)

                self.insert_data(datas)


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
        listt = pro_use.use_proxy(direct_url % (c))
        html = listt[0]
        proxy = listt[1]
        print c
        spider.work(c=c, html=html, proxy=proxy)

