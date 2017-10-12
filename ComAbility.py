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

reload(sys)
sys.setdefaultencoding("utf-8")


LOG_FILENAME = "./log.txt"

target_url = "http://www.cninfo.com.cn"

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


class abSpider():

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
    account_date = "2017-03-31 00:00:00"

    def __init__(self, conn, cursor, period):
        self.conn = conn
        self.cursor = cursor

        self._sess = requests.session()
        self._sess.headers.update({"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36"})

        self.yyyy = period[:4]
        self.mm = period[4:]
        self.post_data = {
            "yyyy": self.yyyy,
            "mm": self.mm,
            "cwzb": self.cwzb,
            "button2": self.button2,
        }
        self.account_date = period + " 00:00:00"
        self.period = period

        self.period_num = 1

        if "03-31" in self.period:
            self.period_num = 1
        elif "06-30" in self.period:
            self.period_num = 2
        elif "09-30" in self.period:
            self.period_num = 3
        elif "12-31" in self.period:
            self.period_num = 4

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
            for r in res:
                if r[0][0] == '0' or r[0][0] == '3':
                    types = "sz"
                else:  # r[0] == '6' or r[0] == '9'
                    types = "sh"
                self.codes.append(
                    {
                        "code": r[0],
                        "date": self.period,
                        "types": types
                    }
                )
        except Exception, e:
            logging.error(e)

    def tofloat(self, fdata):  # 287,670,026.58 -- > 287670026.58
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

    def real_page(self, r):
        body = BeautifulSoup(r.text, "lxml").find("body")
        if body:
            return r
        else:
            script = BeautifulSoup(r.text, "lxml").find("script")
            if not script:
                return r
            redirect_url = self.redirect_url + script.text.split("location.href =")[1].split('\'../..')[1].split('\';')[0]
            # redirect_url = self.redirect_url + script.text.split("location.href =")[1].split("\'")[1]
            r = self._sess.get(redirect_url)
            r.encoding = 'gbk'
            return r

    def work(self, c, proxy):
        code = c["code"]
        types = c["types"]
        dates = c["date"]
        # dates = c["date"].strftime("%Y-%m-%d")
        self.post_data["yyyy"] = dates[:4]
        self.post_data["mm"] = dates[4:]

        self._sess.headers.update({"Referer": "http://www.cninfo.com.cn/information/financialreport/%smb%s.html" % (types, code)})
        post_count = 1
        while True:
            try:
                r = self._sess.post(self.spider_url % (code), data=self.post_data, proxies=proxy, timeout=10)
                break
            except Exception, e:
                post_count += 1
                if post_count > 3:
                    fp = open('lose_ability.txt', 'a')
                    fp.writelines(code + ' post error' + '\n')
                    fp.close()
                    return
                continue
        r = self.real_page(r)
        # print u"judge..........."
        care = 1
        count = 1
        while True:
            if count >= 11:
                care = 2
                break
            if self.post_data['mm'] == '-03-31':
                str1 = 'selected>' + self.post_data['yyyy']
                str2 = u'selected>一季'
                if str1 not in r.text and str2 not in r.text and u"项目 / 报告期" in r.text:
                    count += 1
                    print u"repeat catch"
                    try:
                        r = self._sess.post(self.spider_url % (code), data=self.post_data, proxies=proxy, timeout=10)
                    except Exception, e:
                        continue
                    r = self.real_page(r)
                    continue
                elif str1 in r.text and str2 in r.text and u"项目 / 报告期" in r.text:
                    break
                elif str1 in r.text and str2 in r.text and u"项目 / 报告期" not in r.text:
                    care = 2
                    break
                elif code in r.text and str1 not in r.text:
                    care = 2
                    break
                else:
                    count += 1
                    print u"repeat catch"
                    try:
                        r = self._sess.post(self.spider_url % (code), data=self.post_data, proxies=proxy, timeout=10)
                    except Exception, e:
                        continue
                    r = self.real_page(r)
                    continue
            if self.post_data['mm'] == '-06-30':
                str1 = 'selected>' + self.post_data['yyyy']
                str2 = u'selected>中期'
                if str1 not in r.text and str2 not in r.text and u"项目 / 报告期" in r.text:
                    print u"repeat catch"
                    count += 1
                    try:
                        r = self._sess.post(self.spider_url % (code), data=self.post_data, proxies=proxy, timeout=10)
                    except Exception, e:
                        continue
                    r = self.real_page(r)
                    continue
                elif str1 in r.text and str2 in r.text and u"项目 / 报告期" in r.text:
                    break
                elif str1 in r.text and str2 in r.text and u"项目 / 报告期" not in r.text:
                    care = 2
                    break
                else:
                    count += 1
                    print u"repeat catch"
                    try:
                        r = self._sess.post(self.spider_url % (code), data=self.post_data, proxies=proxy, timeout=10)
                    except Exception, e:
                        continue
                    r = self.real_page(r)
                    continue
            if self.post_data['mm'] == '-09-30':
                str1 = 'selected>' + self.post_data['yyyy']
                str2 = u'selected>三季'
                if str1 not in r.text and str2 not in r.text and u"项目 / 报告期" in r.text:
                    print u"repeat catch"
                    count += 1
                    try:
                        r = self._sess.post(self.spider_url % (code), data=self.post_data, proxies=proxy, timeout=10)
                    except Exception, e:
                        continue
                    r = self.real_page(r)
                    continue
                elif str1 in r.text and str2 in r.text and u"项目 / 报告期" in r.text:
                    break
                elif str1 in r.text and str2 in r.text and u"项目 / 报告期" not in r.text:
                    care = 2
                    break
                else:
                    count += 1
                    print u"repeat catch"
                    try:
                        r = self._sess.post(self.spider_url % (code), data=self.post_data, proxies=proxy, timeout=10)
                    except Exception, e:
                        continue
                    r = self.real_page(r)
                    continue
            if self.post_data['mm'] == '-12-31':
                str1 = 'selected>' + self.post_data['yyyy']
                str2 = u'selected>年度'
                if str1 not in r.text and str2 not in r.text and u"项目 / 报告期" in r.text:
                    print u"repeat catch"
                    count += 1
                    try:
                        r = self._sess.post(self.spider_url % (code), data=self.post_data, proxies=proxy, timeout=10)
                    except Exception, e:
                        continue
                    r = self.real_page(r)
                    continue
                elif str1 in r.text and str2 in r.text and u"项目 / 报告期" in r.text:
                    break
                elif str1 in r.text and str2 in r.text and u"项目 / 报告期" not in r.text:
                    care = 2
                    break
                else:
                    count += 1
                    print u"repeat catch"
                    try:
                        r = self._sess.post(self.spider_url % (code), data=self.post_data, proxies=proxy, timeout=10)
                    except Exception, e:
                        continue
                    r = self.real_page(r)
                    continue
        # print u"end judge..........."
        if care == 2:
            fp = open('lose_ability.txt', 'a')
            fp.writelines(code+'\n')
            fp.close()
            return
        soup = BeautifulSoup(r.text, "lxml")

        soup = self.real_page(soup)  # 检查页面是否需要跳转

        div = soup.find("div", attrs={"class": "zx_left"})
        if not div:
            return
        tbs = div.find_all("table")
        if len(tbs) == 0:
            return
        trs = tbs[0].find_all("tr")
        datas = []
        line = 0
        for tr in trs:
            if line == 0: #忽略第一行
                line = 1
                continue
            tds = tr.find_all("td")
            for td in tds:
                if td.get('rowspan') is None:
                    datas.append(td.text)

        result = {}
        for i in range(0, len(datas), 2):
            if i != len(datas) - 1:
                result[datas[i].strip()] = round(self.tofloat(datas[i + 1]), 2) if datas[i+1].strip() != '' else "NULL"
        '''
        先入mongo测试
        '''
        result['股票信息'] = c
        comAbility.insert(result)


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S', filename=LOG_FILENAME, filemode='w')

    client = pymongo.MongoClient('mongodb://localhost:27017')
    dataCenter = client['dataCenter']
    comAbility = dataCenter['comAbility']

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

    spider = abSpider(conn, cursor, '2017-03-31')

    spider.get_codes()

    print len(spider.codes)

    for c in spider.codes:
        proxy = pro_use.use_proxy()
        print proxy
        spider.work(c, proxy)


