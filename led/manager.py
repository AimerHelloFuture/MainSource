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

LOG_FILENAME = "./log_manager.txt"

target_url = "http://stockpage.10jqka.com.cn/"

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11'
    }

direct_url = "http://stockpage.10jqka.com.cn/%s/company"

peo_num = 0
pos_num = 0
rew_num = 0

end_date = '2017-07-27 00:00:00'   # 即要指定日期爬取这些日期创建的股票的高管信息
import time
today = time.strftime("%Y-%m-%d")  # 为今天日期，用于去数据库中查找今天上市的股票的高管信息


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

    def __init__(self, conn, cursor):
        self.conn = conn
        self.cursor = cursor

        self.codes = []

    def __del__(self):
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

    def get_new_codes(self):
        select_sql = "select sec_code from sec_basic_info where createtime > %s"
        try:
            cur = self.cursor
            cur.execute(select_sql, end_date)
            res = cur.fetchall()
            # res = cur.fetchone()
            for r in res:
                if r[0] != u'':
                    self.codes.append(r[0])
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

    def insert_data(self, datas, new_num, com_source, com_uni_code):

        global peo_num, pos_num, rew_num

        keys_peo = "people_char, come_source, peo_name, birth_day, sex_par, high_edu, back_gro"
        number_peo = "%s, %s, %s, %s, %s, %s, %s"

        keys_pos = "com_uni_code, peo_uni_code, decl_date, led_name, seq_num, Post_code, in_date, off_date, post_type, come_source, post_name"
        number_pos = "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s"

        keys_rew = "com_uni_code, end_date, decl_date, peo_uni_code, led_name, post_name, Post_code, end_shr, com_rwd, come_source"
        number_rew = "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s"

        for i in range(0, len(datas), len(datas) / new_num):
            '''
            插入人物信息表
            '''
            values_peo = [1012003, com_source]
            values_peo.append(datas[i + 0])
            values_peo.append(datas[i + 6])
            values_peo.append(datas[i + 5])
            values_peo.append(datas[i + 7])
            values_peo.append(datas[i + 10])

            ser_sql = 'select peo_uni_code from people_basic_info where peo_name = %s and back_gro = %s'
            self.cursor.execute(ser_sql, (values_peo[2], values_peo[6]))
            res2 = self.cursor.fetchall()
            if len(res2) < 1:                                 # 用于比较人物表中是否存在同一个人任职几家公司查找peo_uni_code
                insert_peo_sql = "insert into people_basic_info (" + keys_peo + ")" + " values (" + number_peo + ")"
                try:
                    self.cursor.execute(insert_peo_sql, values_peo)         # 插入人物信息表
                    peo_num += 1
                    peo_uni_code = self.cursor.lastrowid
                    self.conn.commit()
                except Exception, e:
                    logging.error(e)
            else:
                peo_uni_code = res2[0][0]

            '''
            插入高管持股及薪酬表
            '''
            values_rew = [com_uni_code, self.date2stamp('2016-12-31')]      # 高管持股及薪酬表数据编辑要求设置为2016-12-31
            values_rew.append(datas[i + 2])
            values_rew.append(peo_uni_code)
            values_rew.append(datas[i + 0])
            values_rew.append(datas[i + 1])
            values_rew.append(self.find_poscode(datas[i + 1]))
            values_rew.append(datas[i + 9])
            values_rew.append(datas[i + 8])
            values_rew.append(com_source)
            insert_rew_sql = "insert into com_led_rewardstat (" + keys_rew + ")" + " values (" + number_rew + ")"
            try:
                self.cursor.execute(insert_rew_sql, values_rew)  # 插入高管持股及薪酬表
                rew_num += 1
                self.conn.commit()
            except Exception, e:
                logging.error(e)

            '''
            插入高管任职情况表
            '''
            seq_num = 0
            zhiwu_num = datas[i + 1].count(u',') + 1
            zhiwus = datas[i + 1].split(u',')
            for j in range(0, zhiwu_num):
                post_name = zhiwus[j]
                post_type = datas[i + 11][j]
                Post_code = self.find_poscode(post_name)
                in_date = datas[i + 3][j]
                off_date = datas[i + 4][j]
                led_name = datas[i + 0]
                decl_date = datas[i + 2]
                seq_num += 1
                values_pos = [com_uni_code, peo_uni_code, decl_date, led_name, seq_num, Post_code, in_date, off_date, post_type, com_source, post_name]
                insert_pos_sql = "insert into com_led_position (" + keys_pos + ")" + " values (" + number_pos + ")"
                try:
                    self.cursor.execute(insert_pos_sql, values_pos)  # 插入高管任职情况表
                    pos_num += 1
                    self.conn.commit()
                except Exception, e:
                    logging.error(e)

    def find_poscode(self, post_name):
        pos_sql = 'select system_uni_code from system_const where system_name = %s'
        self.cursor.execute(pos_sql, (post_name))
        res = self.cursor.fetchall()
        if len(res) == 0:
            pos_code = 0
        else:
            pos_code = res[0][0]
        return pos_code

    def date2stamp(self, date):
        try:
            return_date = datetime.strptime(date + " 00:00:00", '%Y-%m-%d %H:%M:%S')
        except Exception, e:
            return_date = None
        return return_date

    def work(self, c, html, year, com_uni_code):

        soup = BeautifulSoup(html.text, 'lxml')

        gsxx = soup.find('div', class_='main_content')

        select_sql1 = "select * from com_led_rewardstat where com_uni_code = %s"
        try:
            cur = self.cursor
            cur.execute(select_sql1, com_uni_code)
            res = cur.fetchall()
            # res = cur.fetchone()
            if len(res) > 0:
                return
        except Exception, e:
            logging.error(e)
            return

        com_source_url = u'同花顺' + direct_url % c
        try:
            lis = gsxx.find('div', id='manager').find('div', class_='bd').find('div', class_='m_tab').find('ul').find_all('li', recursive=False)
        except Exception, e:
            lis = None
        if lis is not None:
            datas = []
            djg_zhiwu = []
            names = []
            length = len(lis)
            new_num = 0
            for i in range(0, length):
                text = lis[i].text.strip().replace('\n', '')[0:2]
                djg_zhiwu.append(text)
            djg = gsxx.find('div', id='manager').find('div', class_='bd').find_all('div', class_='m_tab_content')
            for i in range(0, length):
                trs = djg[i].find('table').find('tbody').find_all('tr', recursive=False)
                for j in range(0, len(trs)):
                    tds = trs[j].find_all('td', recursive=False)
                    for k in range(0, len(tds)):
                        table = tds[k].find('table', class_='m_table ggintro')
                        if table is not None:
                            thead = table.find('thead')
                            thead_tds = thead.find_all('td')
                            name = thead_tds[0].text.strip().replace('\n', '')
                            if name not in names:
                                new_num += 1
                                names.append(name)
                                datas.append(name)                                                      # 1:name

                                zhiwu = thead_tds[1].text.strip().replace('\n', '').replace(' ', '')
                                zhiwu_num = zhiwu.count(u',') + 1
                                datas.append(zhiwu)                                                     # 2:zhiwu

                                print name, zhiwu

                                gonggaoriqi = thead_tds[2].text.strip().replace('\n', '').replace(' ', '')
                                gonggaoriqi = gonggaoriqi.split(u'：')[1]
                                gonggaoriqi = self.date2stamp(gonggaoriqi)
                                datas.append(gonggaoriqi)                                               # 3:gonggaoriqi

                                renqi = thead_tds[3].text.strip().replace('\n', '').replace(' ', '')
                                in_date = renqi.split(u'：')[1].split(u'至')[0]
                                in_date = self.date2stamp(in_date)
                                off_date = renqi.split(u'：')[1].split(u'至')[1]
                                off_date = self.date2stamp(off_date)
                                in_dates = []
                                off_dates = []
                                for mm in range(0, zhiwu_num):
                                    in_dates.append(in_date)
                                    off_dates.append(off_date)
                                datas.append(in_dates)                                                  # 4:in_dates
                                datas.append(off_dates)                                                 # 5:off_dates

                                sex_nian_xue = thead_tds[4].text.strip().split(u' ')
                                name_nian_xue = list(set(sex_nian_xue))
                                name_nian_xue.sort(key=sex_nian_xue.index)
                                try:
                                    name_nian_xue.remove(u'')
                                except Exception, e:
                                    pass
                                xuewei_sql = 'select system_uni_code from system_const where system_name = %s'
                                if len(name_nian_xue) == 0:
                                    datas.append(None)                                                  # 6:sex
                                    datas.append(None)                                                  # 7:nian
                                    datas.append(1010000)                                                  # 8:xue
                                elif len(name_nian_xue) == 1:
                                    if u'男' in name_nian_xue or u'女' in name_nian_xue:
                                        datas.append(name_nian_xue[0])
                                        datas.append(None)
                                        datas.append(1010000)
                                    elif u'岁' in name_nian_xue[0]:
                                        datas.append(None)
                                        chushengriqi = year - int(name_nian_xue[0].split(u'岁')[0])
                                        datas.append(chushengriqi)
                                        datas.append(1010000)
                                    else:
                                        datas.append(None)
                                        datas.append(None)
                                        self.cursor.execute(xuewei_sql, (name_nian_xue[0]))
                                        res = self.cursor.fetchall()
                                        if len(res) == 0:
                                            datas.append(1010000)
                                        else:
                                            datas.append(res[0][0])
                                elif len(name_nian_xue) == 2:
                                    if u'男' not in name_nian_xue and u'女' not in name_nian_xue:
                                        datas.append(None)
                                        chushengriqi = year - int(name_nian_xue[0].split(u'岁')[0])
                                        datas.append(chushengriqi)
                                        self.cursor.execute(xuewei_sql, (name_nian_xue[1]))
                                        res = self.cursor.fetchall()
                                        if len(res) == 0:  # 不存在该学历对应代码
                                            datas.append(1010000)
                                        else:
                                            datas.append(res[0][0])
                                    elif u'岁' not in name_nian_xue[0] and u'岁' not in name_nian_xue[1]:
                                        datas.append(name_nian_xue[0])
                                        datas.append(None)
                                        self.cursor.execute(xuewei_sql, (name_nian_xue[1]))
                                        res = self.cursor.fetchall()
                                        if len(res) == 0:  # 不存在该学历对应代码
                                            datas.append(1010000)
                                        else:
                                            datas.append(res[0][0])
                                    else:
                                        datas.append(name_nian_xue[0])
                                        chushengriqi = year - int(name_nian_xue[1].split(u'岁')[0])
                                        datas.append(chushengriqi)
                                        datas.append(1010000)
                                else:
                                    datas.append(name_nian_xue[0])
                                    chushengriqi = year - int(name_nian_xue[1].split(u'岁')[0])
                                    datas.append(chushengriqi)
                                    self.cursor.execute(xuewei_sql, (name_nian_xue[2]))
                                    res = self.cursor.fetchall()
                                    if len(res) == 0:  # 不存在该学历对应代码
                                        datas.append(1010000)
                                    else:
                                        datas.append(res[0][0])

                                xinchou = thead_tds[5].text.strip().replace('\n', '').replace(' ', '')
                                if u'万元' in xinchou:
                                    datas.append(xinchou.split(u'：')[1].split(u'万元')[0])                # 9:xinchou
                                else:
                                    datas.append(None)

                                chigushu = thead_tds[6].text.strip().replace('\n', '').replace(' ', '')
                                if u'万股' in chigushu:
                                    datas.append(chigushu.split(u'：')[1].split(u'万股')[0])               # 10:chigushu
                                else:
                                    datas.append(None)

                                intro = table.find('tbody').find('p')
                                if intro is not None:
                                    datas.append(intro.text.strip().replace('\n', '').replace(' ', '').replace('\t', ''))  # 11:jianli
                                else:
                                    datas.append(None)

                                zhiwu_leixing = []
                                for mm in range(0, zhiwu_num):
                                    zhiwu_leixing.append(djg_zhiwu[i])
                                datas.append(zhiwu_leixing)                                                # 12:zhiwuleixing

                            else:
                                index = datas.index(name)
                                zhiwu = thead_tds[1].text.strip().replace('\n', '').replace(' ', '')
                                print name, zhiwu
                                zhiwu_num = zhiwu.count(u',') + 1
                                datas[index+1] = datas[index+1] + u',' + zhiwu

                                renqi = thead_tds[3].text.strip().replace('\n', '').replace(' ', '')
                                in_date = renqi.split(u'：')[1].split(u'至')[0]
                                in_date = self.date2stamp(in_date)
                                off_date = renqi.split(u'：')[1].split(u'至')[1]
                                off_date = self.date2stamp(off_date)
                                for mm in range(0, zhiwu_num):
                                    datas[index + 3].append(in_date)
                                    datas[index + 4].append(off_date)
                                    datas[index + 11].append(djg_zhiwu[i])
            self.insert_data(datas, new_num, com_source_url, com_uni_code)


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

    spider = managerSpider(conn, cursor)
    spider.get_new_codes()
    com_uni_codes = []
    for c in spider.codes[0:]:

        select_sql1 = "select com_uni_code from sec_basic_info where sec_code = %s"
        cursor.execute(select_sql1, c)
        res = cursor.fetchall()
        com_uni_code = res[0][0]
        if com_uni_code not in com_uni_codes:
            com_uni_codes.append(com_uni_code)
            html = pro_use.use_proxy(direct_url % (c))
            print c
            spider.work(c=c, html=html, year=2017, com_uni_code=com_uni_code)

    print peo_num, pos_num, rew_num
