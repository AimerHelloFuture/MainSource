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

LOG_FILENAME = "./log_com_profit_sina.txt"

target_url = "http://finance.sina.com.cn/"

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11'
    }

normal_num = 0
bank_num = 0
secrity_num = 0
insurance_num = 0
profit_num = 0

end_date = '2017-09-30'   # 即要爬取的报告期
import time
today = time.strftime("%Y-%m-%d")  # 为今天日期，用于去数据库中stmt_end_date查找实际披露日期小于今天的股票且日期不为0000-00-00 00:00:00


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

    spider_url = "http://money.finance.sina.com.cn/corp/go.php/vFD_ProfitStatement/stockid/%s/ctrl/part/displaytype/1.phtml"

    account_date = "2017-03-31 00:00:00"

    announcement_date = "2017-03-31 00:00:00"

    normal_map = {
        u"公司统一编码": "com_uni_code",
        u"截止日期": "end_date",
        u"一、营业总收入": "overall_income",
        u"营业收入": "main_income",
        u"二、营业总成本": "overall_cost",
        u"营业成本": "main_cost",
        u"营业税金及附加": "tax",
        u"销售费用": "sale_cost",
        u"管理费用": "manage_cost",
        u"财务费用": "fin_cost",
        u"资产减值损失": "asset_loss",
        u"公允价值变动收益": "value_gains",
        u"投资收益": "invest_income",
        u"其中:对联营企业和合营企业的投资收益": "relate_invest_income",
        u"汇兑收益": "gain_loss_income",
        u"三、营业利润": "overall_profit",
        u"加:营业外收入": "addition_income",
        u"减：营业外支出": "addition_cost",
        u"其中：非流动资产处置损失": "interest_dispose",
        u"四、利润总额": "profit_total",
        u"减：所得税费用": "reduce_tax",
        u"五、净利润": "netprofit",
        u"归属于母公司所有者的净利润": "parent_netprofit",
        u"少数股东损益": "minority_loss",
        u"基本每股收益(元/股)": "basic_perstock_income",
        u"稀释每股收益(元/股)": "reduce_perstock_income",
        u"七、其他综合收益": "other_composite_loss",
        u"八、综合收益总额": "all_income_total",
        u"归属于母公司所有者的综合收益总额": "income_for_parent",
        u"归属于少数股东的综合收益总额": "income_for_minority"
    }

    bank_map = {
        u"公司统一编码": "com_uni_code",
        u"截止日期": "end_date",
        u"一、营业收入": "overall_income",
        u"利息净收入": "interest_netincome",
        u"其中：利息收入": "interest_income",
        u"减：利息支出": "interest_cost",
        u"手续费及佣金净收入": "commission_netincome",
        u"其中:手续费及佣金收入": "commission_income",
        u"减：手续费及佣金支出": "commission_cost",
        u"投资净收益": "invest_income",
        u"其中:对联营公司的投资收益": "relate_invest_income",
        u"汇兑收益": "gain_loss_income",
        u"公允价值变动净收益": "value_gains",
        u"其他业务收入": "other_income",
        u"二、营业支出": "operating_payout",
        u"营业税金及附加": "tax",
        u"业务及管理费用": "operating_manage_cost",
        u"资产减值损失": "asset_loss",
        u"其他业务支出": "other_bus_cost",
        u"三、营业利润": "overall_profit",
        u"加:营业外收入": "addition_income",
        u"减:营业外支出": "addition_cost",
        u"四、利润总额": "profit_total",
        u"减:所得税": "reduce_tax",
        u"五、净利润": "netprofit",
        u"归属于母公司的净利润": "parent_netprofit",
        u"少数股东权益": "minority_loss",
        u"基本每股收益(元/股)": "basic_perstock_income",
        u"稀释每股收益(元/股)": "reduce_perstock_income",
        u"七、其他综合收益": "other_composite_loss",
        u"八、综合收益总额": "all_income_total",
        u"归属于母公司所有者的综合收益总额": "income_for_parent",
        u"归属于少数股东的综合收益总额": "income_for_minority"
    }

    secrity_map = {
        u"公司统一编码": "com_uni_code",
        u"截止日期": "end_date",
        u"一、营业收入": "main_income",
        u"手续费及佣金净收入": "commission_netincome",
        u"代理买卖证券业务净收入": "agent_secrtities_sale_netincome",
        u"证券承销业务净收入": "secrities_under_income",
        u"受托客户资产管理业务净收入": "asset_management_netincome",
        u"利息净收入": "interest_netincome",
        u"投资收益": "invest_income",
        u"其中:对联营企业和合营企业的投资收益": "relate_invest_income",
        u"汇兑损益": "gain_loss_income",
        u"公允价值变动损益": "value_gains",
        u"其他业务收入": "other_income",
        u"二、营业支出": "operating_payout",
        u"营业税金及附加": "tax",
        u"管理费用": "operating_manage_cost",
        u"资产减值损失": "asset_loss",
        u"其他业务成本": "other_bus_cost",
        u"三、营业利润": "overall_profit",
        u"加:营业外收入": "addition_income",
        u"减:营业外支出": "addition_cost",
        u"四、利润总额": "profit_total",
        u"减:所得税": "reduce_tax",
        u"五、净利润": "netprofit",
        u"归属于母公司所有者的净利润": "parent_netprofit",
        u"少数股东损益": "minority_loss",
        u"基本每股收益(元/股)": "basic_perstock_income",
        u"稀释每股收益(元/股)": "reduce_perstock_income",
        u"七、其他综合收益": "other_composite_loss",
        u"八、综合收益总额": "all_income_total",
        u"归属于母公司所有者的综合收益总额": "income_for_parent",
        u"归属于少数股东的综合收益总额": "income_for_minority"
    }

    insurance_map = {
        u"公司统一编码": "com_uni_code",
        u"截止日期": "end_date",
        u"一、营业收入": "overall_income",
        u"已赚保费": "earn_insurance",
        u"保费业务收入": "insurance_bus_income",
        u"其中:分保费收入": "premium_income",
        u"减:分出保费": "separate_premiums",
        u"提取未到期责任准备金": "extraction_unexpired",
        u"投资净收益": "invest_income",
        u"其中:对联营企业和合营企业的投资损失": "relate_invest_income",
        u"公允价值变动损益": "change_income",
        u"汇兑损益": "gain_loss_income",
        u"其他业务收入": "other_income",
        u"二、营业支出": "operating_payout",
        u"退保金": "canel_insurance_money",
        u"赔付支出": "pay_expenses",
        u"减:摊回赔付支出": "spread_pay_expenses",
        u"提取保险责任准备金": "insurance_liability",
        u"减:摊回保险责任准备金": "spread_insurance_liability",
        u"保户红利支出": "insurance_cost",
        u"分保费用": "reduce_insurance_cost",
        u"营业税金及附加": "tax",
        u"手续费及佣金支出": "commission_cost",
        u"管理费用": "operating_manage_cost",
        u"减:摊回分保费用": "spread_premium",
        u"资产减值损失": "asset_loss",
        u"其他业务成本": "other_bus_cost",
        u"三、营业利润": "overall_profit",
        u"加:营业外收入": "addition_income",
        u"减:营业外支出": "addition_cost",
        u"四、利润总额": "profit_total",
        u"减:所得税费用": "reduce_tax",
        u"五、净利润": "netprofit",
        u"归属于母公司股东的净利润": "parent_netprofit",
        u"少数股东损益": "minority_loss",
        u"基本每股收益(元/股)": "basic_perstock_income",
        u"稀释每股收益(元/股)": "reduce_perstock_income",
        u"七、其他综合收益": "other_composite_loss",
        u"八、综合收益总额": "all_income_total",
        u"归属于母公司所有者的综合收益总额": "income_for_parent",
        u"归属于少数股东的综合收益总额": "income_for_minority"
    }

    profit_map = {
        u"公司统一编码": "com_uni_code",
        u"截止日期": "end_date",
        u"公告日期": "announcement_date",

        u"一、营业总收入": "overall_income",

        u"营业收入": "main_income",
        u"一、营业收入": "main_income",

        u"利息净收入": "interest_netincome",

        u"其中：利息收入": "interest_income",
        u"其中:利息收入": "interest_income",

        u"减：利息支出": "interest_cost",
        u"利息支出": "interest_cost",

        u"手续费及佣金净收入": "commission_netincome",

        u"其中:手续费及佣金收入": "commission_income",

        u"减：手续费及佣金支出": "commission_cost",
        u"手续费及佣金支出": "commission_cost",

        u"已赚保费": "earn_insurance",
        u"保费业务收入": "insurance_bus_income",

        u"其中:分保费收入": "premium_income",

        u"减:分出保费": "separate_premiums",

        u"提取未到期责任准备金": "extraction_unexpired",
        u"受托客户资产管理业务净收入": "netincome_of_asset_management_fees",

        u"投资收益": "invest_income",
        u"投资净收益": "invest_income",

        u"其中:对联营企业和合营企业的投资收益": "relate_invest_income",
        u"其中:对联营企业和合营企业的投资损失": "relate_invest_income",
        u"其中:对联营公司的投资收益": "relate_invest_income",

        u"汇兑收益": "gain_loss_income",
        u"汇兑损益": "gain_loss_income",

        u"公允价值变动收益": "value_gains",
        u"公允价值变动损益": "value_gains",
        u"公允价值变动净收益": "value_gains",

        u"其他业务收入": "other_income",

        u"二、营业总成本": "overall_cost",
        u"营业成本": "main_cost",
        u"二、营业支出": "operat_expenses",
        u"退保金": "canel_insurance_money",
        u"赔付支出": "pay_expenses",
        u"减:摊回赔付支出": "spread_pay_expenses",
        u"提取保险责任准备金": "insurance_liability",
        u"保户红利支出": "insurance_cost",
        u"分保费用": "reduce_insurance_cost",

        u"营业税金及附加": "tax",
        u"业务及管理费用": "operating_manage_cost",
        u"减:摊回分保费用": "spread_premium",
        u"销售费用": "sale_cost",
        u"管理费用": "manage_cost",
        u"财务费用": "fin_cost",
        u"资产减值损失": "asset_loss",

        u"其他业务支出": "other_bus_cost",
        u"其他业务成本": "other_bus_cost",

        u"三、营业利润": "overall_profit",
        u"加:营业外收入": "addition_income",

        u"减：营业外支出": "addition_cost",
        u"减:营业外支出": "addition_cost",

        u"其中：非流动资产处置损失": "disposal_noncurrent_assets",

        u"四、利润总额": "profit_total",

        u"减：所得税费用": "reduce_tax",
        u"减:所得税": "reduce_tax",
        u"减:所得税费用": "reduce_tax",

        u"五、净利润": "netprofit",

        u"归属于母公司所有者的净利润": "parent_netprofit",
        u"归属于母公司的净利润": "parent_netprofit",
        u"归属于母公司股东的净利润": "parent_netprofit",

        u"少数股东损益": "minority_loss",
        u"少数股东权益": "minority_loss",

        u"基本每股收益(元/股)": "basic_perstock_income",
        u"稀释每股收益(元/股)": "reduce_perstock_income",
        u"七、其他综合收益": "other_composite_loss",
        u"八、综合收益总额": "all_income_total",
        u"归属于母公司所有者的综合收益总额": "income_for_parent",
        u"归属于少数股东的综合收益总额": "income_for_minority"
    }

    def __init__(self, conn, cursor, period, announcement):
        self.conn = conn
        self.cursor = cursor

        self.account_date = period + " 00:00:00"
        self.period = period
        self.announcement = announcement + " 00:00:00"
        self.zero_date = '0000-00-00' + " 00:00:00"

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
        select_sql = "select sec_uni_code,stmt_true_plan_data from stmt_end_date where stmt_true_plan_data <= %s and stmt_true_plan_data > %s and end_date = %s"
        try:
            cur = self.cursor
            cur.execute(select_sql, (self.announcement, self.zero_date, self.account_date))
            res = cur.fetchall()
            select_sql2 = "select com_uni_code,sec_code from sec_basic_info where sec_uni_code = %s"
            for r in res:
                sec_uni_code = r[0]
                stmt_true_plan_data = r[1]
                cur.execute(select_sql2, (sec_uni_code))
                res2 = cur.fetchall()
                com_uni_code = res2[0][0]
                sec_code = res2[0][1]
                if sec_code[0] == '0' or sec_code[0] == '3':
                    types = "sz"
                else:  # r[0] == '6' or r[0] == '9'
                    types = "sh"
                self.codes.append(
                    {
                        "code": sec_code,
                        "date": self.period,
                        "types": types,
                        "com_uni_code": com_uni_code,
                        "stmt_true_plan_data": stmt_true_plan_data
                    }
                )
        except Exception, e:
            logging.error(e)

    def insert_data(self, result, fin_repo_type, com_source_url):

        global normal_num, bank_num, secrity_num, insurance_num, profit_num
        keys = "report_type, principles, consolidation, come_source, currency_code, report_format"
        principles = 1502002
        consolidation = 1501002
        come_source = u'新浪财经' + com_source_url
        currency_code = 1011001
        values = [self.report_type, principles, consolidation, come_source, currency_code]
        number = "%s, %s, %s, %s, %s, %s"

        # remark = '('
        if fin_repo_type == 'normal':
            values.append(1013025)
            normal_num += 1

        elif fin_repo_type == 'bank':
            values.append(1013002)
            bank_num += 1

        elif fin_repo_type == 'secrity':
            values.append(1013006)
            secrity_num += 1

        else:  # fin_repo_type == 'insurance'
            values.append(1013003)
            insurance_num += 1

        for r in result.keys():
            if self.profit_map.get(r):
                keys = keys + "," + self.profit_map[r]
                number = number + "," + "%s"
                # remark = remark + self.normal_map[r] +','
                values.append(result[r])
        insert_sql = "insert into com_profit" + " (" + keys + ")" + " values (" + number + ")"
        try:
            self.cursor.execute(insert_sql, values)
            profit_num += 1
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

    def search_type(self, com_uni_code):
        try:
            search_type_sql = 'select fin_repo_type from com_basic_info where com_uni_code = %s'  # 注意替换为com_basic_info
            self.cursor.execute(search_type_sql, (com_uni_code))
            res2 = self.cursor.fetchall()
            fin_repo_type = res2[0][0]
            if fin_repo_type == u'一般':
                fin_repo_type = 'normal'
            elif fin_repo_type == u'银行':
                fin_repo_type = 'bank'
            elif fin_repo_type == u'证券':
                fin_repo_type = 'secrity'
            else:
                fin_repo_type = 'insurance'
            return fin_repo_type
        except Exception, e:
            fp = open('lose_com_profit_sina.txt', 'a')
            fp.writelines(self.account_date + com_uni_code + ' error' + '\n')
            fp.close()
            return False

    def work(self, c, proxy):
        code = c["code"]
        types = c["types"]
        dates = c["date"]
        stmt_true_plan_data = c["stmt_true_plan_data"]
        com_uni_code = c["com_uni_code"]

        result = {}
        fin_repo_type = self.search_type(com_uni_code)
        if fin_repo_type != False:
            end_date = datetime.strptime(self.account_date, '%Y-%m-%d %H:%M:%S')
            result[u'公司统一编码'] = com_uni_code
            result[u'截止日期'] = end_date
            result[u'公告日期'] = stmt_true_plan_data
        else:
            fin_repo_type = ''
            result = {}

        table_name = 'com_profit'
        come_source_url = self.spider_url % (code)
        # come_source = u'新浪财经' + come_source_url

        # 增量部分：若数据库中已有记录则无需再次插入
        select_sql = 'select * from ' + table_name + ' where com_uni_code = %s and end_date = %s and come_source like %s'
        try:
            self.cursor.execute(select_sql, (result[u'公司统一编码'], result[u'截止日期'], u'新浪财经%'))
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
                    fp = open('lose_com_profit_sina.txt', 'a')
                    fp.writelines(self.account_date + code + ' post error' + '\n')
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
            trs = soup.find('table', id='ProfitStatementNewTable0').find('tbody').find_all('tr', recursive=False)
        except Exception, e:
            trs = None
        if not trs:
            return

        datas = []
        line = 0
        for tr in trs:
            if line == 0: #忽略第一行
                line = 1
                continue
            tds = tr.find_all('td', recursive=False)
            if tds == None:
                continue
            if len(tds) == 2:
                for td in tds:
                    datas.append(td.text.strip().replace('\n', ''))

        for i in range(0, len(datas), 2):
            result[datas[i].strip()] = round(self.tofloat(datas[i + 1]), 2) * 10000.0 if datas[i + 1] != '--' else None

        if fin_repo_type == '':
            result = {}

        self.insert_data(result, fin_repo_type, come_source_url)
        print code, com_uni_code, profit_num, fin_repo_type, normal_num, bank_num, secrity_num, insurance_num

        # self.insert_data(code, result)
        # '''
        # 先入mongo测试
        # '''
        # result['股票信息'] = c
        # profit.insert(result)


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

    spider = balanceSpider(conn, cursor, end_date, today)
    spider.get_codes()
    for c in spider.codes:
        proxy = pro_use.use_proxy()
        # print proxy
        spider.work(c, proxy)
        # print c
