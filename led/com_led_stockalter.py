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


LOG_FILENAME = "./log_stockalter.txt"

target_url = "http://data.cnstock.com/gpsj/ggcg/ggcg.html"

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11'
    }


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
                return html
            except Exception, e:
                continue


class balanceSpider:

    filed_map = {
        u"日期": "end_date",  # 即为mysql中变动日期
        u"公司统一编码": "com_uni_code",
        u"人物统一编码": "peo_uni_code",
        u"变动人": "cha_name",
        u"变动股数": "chng_vol",
        u"成交均价": "cheg_ep",
        u"变动原因": "chan_reason",
        u"变动比例(‰)": "chng_pct",
        u"变动后持股数": "end_vol",
        u'董监高人员姓名': "led_name",
        u"职务": "post_code",
        u"变动人与董监高的关系": "exec_relat",
        u"变动前持股数": "begin_vol",
        u"序号": "seq_sum",
        u'备注': "remark",
        u'人物统一编码': "peo_uni_code"
    }

    def __init__(self, conn, cursor):
        self.conn = conn
        self.cursor = cursor

    def __del__(self):
        self.cursor.close()
        self.conn.commit()
        self.conn.close()

    def insert_data(self, new_num, title, datas):

        for i in range(0, new_num * len(title), len(title)):
            values = []
            keys = "come_source"
            # keys = keys + "," + ""
            values.append(u'中国证券网http://data.cnstock.com/gpsj/ggcg/ggcg.html')
            number = "%s"
            for j in range(0, len(title)):
                if self.filed_map.get(title[j]):
                    keys = keys + "," + self.filed_map[title[j]]
                    number = number + "," + "%s"
                    values.append(datas[i + j])
            insert_sql = "insert into com_led_stockalter (" + keys + ")" + " values (" + number + ")"
            try:
                self.cursor.execute(insert_sql, values)
                self.conn.commit()
            except Exception, e:
                logging.error(e)
                # return

    def string_number(self, s):  # 判断字符串是否为纯数字，是则转换为float，否则不变
        try:
            return float(s)
        except ValueError:
            pass
        # try:
        #     import unicodedata
        #     return unicodedata.numeric(s)
        # except (TypeError, ValueError):
        #     pass

        return s

    def write_lose(self):
        fp = open('lose_stockalter.txt', 'a')
        fp.writelines(datetime.now().strftime('%Y-%m-%d') + '\n')
        fp.close()

    def write_lose_reward3(self, code, name):
        fp = open('lose_stockalter.txt', 'a')
        fp.writelines(code + ' ' + name + ' error' + '\n')
        fp.close()

    def search_com_type(self, code, name):
        try:
            search_com_sql = 'select com_uni_code from sec_basic_info where sec_code = %s'
            self.cursor.execute(search_com_sql, (code))
            res = self.cursor.fetchall()
            com_uni_code = res[0][0]
            search_type_sql = 'select peo_uni_code from com_led_position where com_uni_code = %s and led_name = %s'
            self.cursor.execute(search_type_sql, (com_uni_code, name))
            res2 = self.cursor.fetchall()
            if len(res2) == 0:
                peo_uni_code = 0
            else:
                peo_uni_code = res2[0][0]
            list = [com_uni_code, peo_uni_code]
            return list
        except Exception, e:
            self.write_lose_reward3(code, name)
            return False

    def is_in(self, compare_date, compare_stock, compare_led, compare_vol):  # 用于判断mysql中是否存在该记录，若存在，返回True，否则返回公司统一编码
        select_sql1 = "select com_uni_code from sec_basic_info where sec_code = %s" % (compare_stock)
        try:
            self.cursor.execute(select_sql1)
            res = self.cursor.fetchall()
            com_uni_code = res[0][0]
            date = datetime.strptime(compare_date, '%Y-%m-%d %H:%M:%S')
            select_sql2 = "select * from com_led_stockalter where end_date = %s and com_uni_code = %s and cha_name = %s and chng_vol = %s"
            self.cursor.execute(select_sql2, (date, com_uni_code, compare_led, compare_vol))
            res2 = self.cursor.fetchall()
            if len(res2) >= 1:  # 是否存在记录
                return True
            else:
                return com_uni_code
        except Exception, e:
            logging.error(e)

    def work(self, html):
        try:
            soup = BeautifulSoup(html.content, "lxml")
        except Exception, e:
            self.write_lose()
            return
        try:
            trs = soup.find("table", attrs={"class": "nosortdatalist"}).find('tbody').find_all('tr', recursive=False)
        except Exception, e:
            trs = None
        if not trs:
            self.write_lose()
            return

        title = []  # 表头
        datas = []  # 数据
        new_num = 0  # 新出的变动数据有几条
        '''
        专门获取日期，股票代码，变动人，变动股数，用于去mysql中比较
        下面三个变量用于获取下标
        '''
        # date_num = 0
        # stock_num = 0
        # led_name = 0
        ths = trs[0].find_all('th', recursive=False)
        for i in range(0, len(ths)):
            title_text = ths[i].text.replace(' ', '').replace('\n', '').replace('\t', '')
            if title_text == '代码':
                title.append(u'公司统一编码')
            else:
                title.append(title_text)

        title.append(u'变动前持股数')  # 倒数第三列加入字段变动前持股数
        title.append(u'序号')  # 加入字段序号
        # title.append(u'备注')  # 最后一列加入字段备注，已防止出现职务匹配不上如多个职务或者职务不存在
        title.append(u'人物统一编码')  # 最后一列加入字段备注，已防止出现职务匹配不上如多个职务或者职务不存在

        # 下面两个变量为了计算seq_num
        first_date = trs[1].find_all("td", recursive=False)[0].find('span').get('title') + ' 00:00:00'  # 用于初始日期
        first_stock = trs[1].find_all("td", recursive=False)[1].text  # 用于初始代码
        seq_num = 0
        for i in range(1, len(trs)):
            tds = trs[i].find_all("td", recursive=False)
            compare_date = tds[0].find('span').get('title') + ' 00:00:00'  # 获取日期
            compare_stock = tds[1].text  # 获取股票代码
            compare_led = tds[3].find('span').get('title')  # 获取变动人姓名
            compare_vol = tds[4].text  # 获取变动股数

            djg_name = tds[11].text
            list = self.search_com_type(compare_stock, djg_name)
            if list != False:
                peo_uni_code = list[1]

                if first_date == compare_date and first_stock == compare_stock:
                    seq_num += 1
                else:
                    first_date = compare_date
                    first_stock = compare_stock
                    seq_num = 1

                ret = self.is_in(compare_date, compare_stock, compare_led, compare_vol)
                if ret == True:
                    break
                else:
                    new_num += 1
                    print new_num, compare_date, compare_stock, compare_led
                    date = datetime.strptime(compare_date, '%Y-%m-%d %H:%M:%S')
                    # datas.append(tds[0].find('span').get('title'))
                    datas.append(date)
                    num_after = 0  # 变动后持股数
                    num_change = 0  # 变动股数
                    beizhu = None
                    for j in range(1, len(tds)):
                        if j == 1:
                            datas.append(ret)  # 插入根据股票代码得到的公司统一代码
                        elif tds[j].text == '-':
                            # datas.append("NULL")
                            datas.append(None)
                            if j == 9:   # 变动后持股数
                                # num_after = 'NULL'
                                num_after = None
                            if j == 4:   # 变动股数
                                # num_change = 'NULL'
                                num_change = None
                        else:
                            if j == 3:  # 变动人因为网页可能显示不全
                                result = compare_led
                            # elif j == 12:  # 得到职务对应常量表的代码，若没有则在备注中添加职务异常
                            #     pos_sql = "select system_uni_code from system_const where system_name = %s"
                            #     self.cursor.execute(pos_sql, (tds[j].text))
                            #     res = self.cursor.fetchall()
                            #     if len(res) == 0:  # 不存在该职务对应代码
                            #         result = 0
                            #         beizhu = '职务常量代码异常，请手动修改'
                            #     else:
                            #         result = res[0][0]
                            #         beizhu = '无'
                            else:
                                result = self.string_number(tds[j].text)
                                if j == 8:
                                    if tds[j].text == '-':
                                        result = None
                                    else:
                                        result = result / 10
                            datas.append(result)
                            if j == 9:
                                num_after = result
                            if j == 4:
                                num_change = result
                    #  减法算出变动前持股数
                    # if num_after != 'NULL' and num_change != 'NULL':
                    if num_after != None and num_change != None:
                        datas.append(num_after + num_change)
                    else:
                        # datas.append('NULL')
                        datas.append(None)
                    datas.append(seq_num)
                    # datas.append(beizhu)
                    datas.append(peo_uni_code)

        self.insert_data(new_num, title, datas)
        # '''
        # 先入mongo测试
        # '''
        # result['数据'] = datas
        # com_led_stockalter.insert(result)


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S', filename=LOG_FILENAME, filemode='w')

    client = pymongo.MongoClient('mongodb://localhost:27017')
    dataCenter = client['dataCenter']
    com_led_stockalter = dataCenter['com_led_stockalter']

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

    spider = balanceSpider(conn, cursor)

    html = pro_use.use_proxy()
    spider.work(html)
