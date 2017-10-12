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

flag = 0  # 用于判断增量
num = 0

LOG_FILENAME = "./log.txt"

target_url = "http://quotes.money.163.com/f10/gszl_000002.html"

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
                return ii
            except Exception, e:
                continue


class balanceSpider:

    spider_url = "http://quotes.money.163.com/service/gsgk.html?symbol=%s&session=%s&duty=%s"

    file_map = {
        u"公司统一编码": "com_uni_code",
        u"截止日期": "end_date",
        u"销售商品、提供劳务收到的现金": "sale_cash",
        u"收到的税费返还": "tax_return",
        u"收到其他与经营活动有关的现金": "rec_other_cash",
        u"经营活动现金流入小计": "bussiness_cash_total",
        u"购买商品、接受劳务支付的现金": "buy_for_cash",
        u"支付给职工以及为职工支付的现金": "pay_emp_cash",
        u"支付的各项税费": "pay_tax",
        u"支付其他与经营活动有关的现金": "pay_other_cash",
        u"经营活动现金流出小计": "bussiness_cash_output",
        u"经营活动产生的现金流量净额": "bussiness_cash_netvalue",
        u"收回投资收到的现金": "rec_invest_cash",
        u"取得投资收益收到的现金": "invest_rec_cash",
        u"处置固定资产、无形资产和其他长期资产收回的现金净额": "dispose_asset_netvalue",
        u"处置子公司及其他营业单位收到的现金净额": "subs_net_cash",
        u"收到其他与投资活动有关的现金": "rec_otherinvest_cash",
        u"投资活动现金流入小计": "invest_cash_total",
        u"购建固定资产、无形资产和其他长期资产支付的现金": "buy_asset_cash",
        u"投资支付的现金": "invest_pay_cash",
        u"取得子公司及其他营业单位支付的现金净额": "subs_pay_netcash",
        u"支付其他与投资活动有关的现金": "pay_otherinvest_cash",
        u"投资活动现金流出小计": "invest_cash_output",
        u"投资活动产生的现金流量净额": "invest_cash_netvalue",
        u"吸收投资收到的现金": "rec_invest_reccash",
        u"取得借款收到的现金": "rec_borrow_cash",
        u"收到其他与筹资活动有关的现金": "rec_other_relatecash",
        u"筹资活动现金流入小计": "borrow_cash_total",
        u"偿还债务支付的现金": "pay_debet_cash",
        u"分配股利、利润或偿还利息支付的现金": "interest_pay_cash",
        u"支付其他与筹资活动有关的现金": "pay_other_relatecash",
        u"筹资活动现金流出小计": "borrow_cash_outtotal",
        u"筹资活动产生的现金流量净额": "borrow_cash_netvalue",
        u"四、汇率变动对现金的影响": "rate_to_cash",
        u"五、现金及现金等价物净增加额": "cash_to_netadd",
        u"期初现金及现金等价物余额": "origin_cash",
        u"期末现金及现金等价物余额": "last_cash"
    }

    def __init__(self, conn, cursor):
        self.conn = conn
        self.cursor = cursor
        self._sess = requests.session()
        self._sess.headers.update({"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36"})

        self.codes = []

    def __del__(self):
        self._sess.close()
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

    def insert_data(self, datas, com_source_url):
        global flag, num
        keys = "come_source"
        come_source = '网易财经' + com_source_url
        values = [come_source]
        number = "%s"

        # 增量部分：若数据库中已有记录则无需再次插入
        select_sql = 'select * from com_led_rewardstat where end_date = %s and com_uni_code = %s and peo_uni_code = %s'
        try:
            self.cursor.execute(select_sql, (datas[2], datas[0], datas[1]))
            self.conn.commit()
        except Exception, e:
            logging.error(e)
        res_s = self.cursor.fetchall()
        if len(res_s) > 0:
            return
        flag = 1
        for i in range(0, len(datas), 7):
            keys = keys + ',com_uni_code,peo_uni_code,end_date,led_name,Post_code,end_shr,com_rwd'
            for j in range(0, 7):
                values.append(datas[i+j])
            number = number + ',%s,%s,%s,%s,%s,%s,%s'
            insert_sql = "insert into com_led_rewardstat (" + keys + ")" + " values (" + number + ")"
            try:
                num += 1
                print num
                self.cursor.execute(insert_sql, values)
                self.conn.commit()
            except Exception, e:
                logging.error(e)

    def tofloat(self, fdata): # 287,670,026.58 -- > 287670026.58
        if fdata[0] != "-":
            fs = fdata.split(",")
            flen = len(fs)
            result = 0
            for f in fs:
                result = result + float(f)
                if flen == 1:
                    return result
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
                    return result * -1.0
                else:
                    result = result * 1000.0
                    flen = flen - 1

    def search_com_type(self, code, name):
        try:
            search_com_sql = 'select com_uni_code from sec_basic_info where sec_code = %s'
            self.cursor.execute(search_com_sql, (code))
            res = self.cursor.fetchall()
            com_uni_code = res[0][0]
            search_type_sql = 'select peo_uni_code from com_led_position where com_uni_code = %s and led_name = %s'
            self.cursor.execute(search_type_sql, (com_uni_code, name))
            res2 = self.cursor.fetchall()
            peo_uni_code = res2[0][0]
            list = [com_uni_code, peo_uni_code]
            return list
        except Exception, e:
            self.write_lose_reward3(code, name)
            return False

    def write_lose_reward(self, code):
        fp = open('lose_rewardstat.txt', 'a')
        fp.writelines(code + ' get div error' + '\n')
        fp.close()

    def write_lose_reward2(self, code, djg):
        fp = open('lose_rewardstat.txt', 'a')
        fp.writelines(code + ' get ' + djg + ' error' + '\n')
        fp.close()

    def write_lose_reward3(self, code, name):
        fp = open('lose_rewardstat.txt', 'a')
        fp.writelines(code + ' ' + name + ' error' + '\n')
        fp.close()

    def date2stamp(self, date):
        try:
            return_date = datetime.strptime(date + " 00:00:00", '%Y-%m-%d %H:%M:%S')
        except Exception, e:
            return_date = None
        return return_date

    def work(self, c, proxy):
        code = c

        come_source_url = "http://quotes.money.163.com/f10/gszl_%s.html" % (code)

        get_count = 1
        while True:
            try:
                gsgg = self._sess.get(come_source_url, proxies=proxy, timeout=10)
                soup = BeautifulSoup(gsgg.text, "lxml")
                riqi = soup.find("div", attrs={"class": "area"}).find_all("h2", attrs={"class": "title_01"}, recursive=False)
                riqi = riqi[0].find('ul').text.strip().replace('\n', '')
                riqi = unicode(riqi[-10:])
                riqi2 = self.date2stamp(riqi)
                companyTab = soup.find("div", attrs={"class": "area"}).find("div", attrs={"class": "inner_box"}).find("div", attrs={"id": "companyTab"})
                divs = companyTab.find_all('div', recursive=False)
                lis = companyTab.find('ul', attrs={"class": "kchart_tab_title clearfix"}).find_all('li', recursive=False)
                break
            except Exception, e:
                get_count += 1
                if get_count > 10:
                    self.write_lose_reward(code)
                    return
                continue
        djg = []
        length = len(lis)
        names = []
        peo_codes = []
        datas = []
        print code
        for i in range(0, length):
            text = lis[i].text.strip().replace('\n', '')[:-2]
            if text == '董事会':
                djg = 'dsh'
            elif text == '监事会':
                djg = 'jsh'
            elif text == '高管层':
                djg = 'ggc'

            session = divs[i].find('option').get('value')

            self._sess.headers.update({"Referer": come_source_url})
            get_count = 1
            while True:
                try:
                    true = self._sess.get(self.spider_url % (code, session, djg), proxies=proxy, timeout=10)
                    break
                except Exception, e:
                    get_count += 1
                    if get_count > 10:
                        self.write_lose_reward2(code, djg)
                        return
                    continue

            soup = BeautifulSoup(true.content, "lxml")

            trs = soup.find_all('tr')

            for j in range(1, len(trs)):
                tds = trs[j].find_all('td', recursive=False)
                name = tds[0].text.strip().replace('\n', '')
                zhiwu = tds[1].text.strip().replace('\n', '')
                chigushu = tds[3].text.strip().replace('\n', '')
                baochou = tds[4].text.strip().replace('\n', '')
                chigushu = self.tofloat(chigushu) if chigushu != '--' else None
                baochou = self.tofloat(baochou) if baochou != '--' else None
                if name not in names:
                    code_list = self.search_com_type(code, name)
                    if code_list != False:
                        com_uni_code = code_list[0]
                        peo_uni_code = code_list[1]
                        names.append(name)
                        peo_codes.append(peo_uni_code)
                        datas.append(com_uni_code)
                        datas.append(peo_uni_code)
                        datas.append(riqi2)
                        datas.append(name)
                        datas.append(zhiwu)
                        datas.append(chigushu)
                        datas.append(baochou)
                    else:
                        continue
                else:
                    index = datas.index(name)
                    datas[index+1] = datas[index+1] + ',' + zhiwu
        self.insert_data(datas, come_source_url)
        # '''
        # 先入mongo测试
        # '''
        # result['股票信息'] = c
        # cash.insert(result)


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S', filename=LOG_FILENAME, filemode='w')

    client = pymongo.MongoClient('mongodb://localhost:27017')
    dataCenter = client['dataCenter']
    cash = dataCenter['cash']

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
    spider.get_codes()

    for c in spider.codes:
        proxy = pro_use.use_proxy()
        # print proxy
        spider.work(c, proxy)
        # print c
