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

LOG_FILENAME = "./log_com_cash.txt"

target_url = "http://www.cninfo.com.cn"

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11'
    }

normal_num = 0
bank_num = 0
secrity_num = 0
insurance_num = 0
cash_num = 0

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

    spider_url = "http://www.cninfo.com.cn/information/stock/cashflow_.jsp?stockCode=%s"
    redirect_url = "http://www.cninfo.com.cn/information/cashflow"

    yyyy = "2017"
    mm = "-03-31"
    cwzb = "cashflow"
    button2 = "%CC%E1%BD%BB"
    post_data = {
        "yyyy": yyyy,
        "mm": mm,
        "cwzb": cwzb,
        "button2": button2,
    }
    account_date = "2017-03-31 00:00:00"

    announcement_date = "2017-03-31 00:00:00"

    normal_map = {
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

    bank_map = {
        u"公司统一编码": "com_uni_code",
        u"截止日期": "end_date",
        u"客户存款和同业存放款项净增加额": "depo_net_r",
        u"向中央银行借款净增加额": "cen_bank_borr_net_incr",
        u"处置交易性金融资产净增加额": "depo_too_the_rbanks",
        u"拆入资金净增加额": "fdsborr_net_incr",
        u"收取利息、手续费及佣金的现金": "char_inte_cash",
        u"收到其他与经营活动有关的现金": "other_bizget_cash",
        u"经营活动现金流入小计": "bussiness_cash_total",
        u"客户贷款及垫款净增加额": "custom_netvalue",
        u"存放中央银行和同业款项净增加额": "trade_paym_incr",
        u"支付的手续费及佣金的现金": "fee_pay_cash",
        u"支付给职工以及为职工支付的现金": "pay_emp_cash",
        u"支付的各项税费": "pay_tax",
        u"支付其他与经营活动有关的现金": "pay_other_biz_cash",
        u"经营活动现金流出小计": "bussiness_cash_output",
        u"经营活动产生的现金流量净额": "bussiness_cash_netvalue",
        u"收回投资收到的现金": "rec_invest_cash",
        u"取得投资收益收到的现金": "invest_rec_cash",
        u"收到其他与投资活动有关的现金": "rece_other_invest_cash",
        u"投资活动现金流入小计": "invest_cash_total",
        u"投资支付的现金": "invest_pay_cash",
        u"购建固定资产、无形资产和其他长期资产支付的现金": "buy_asset_cash",
        u"支付其他与投资活动有关的现金": "pay_inve_cash",
        u"投资活动现金流出小计": "invest_cash_output",
        u"投资活动产生的现金流量净额": "invest_cash_netvalue",
        u"发行债券收到的现金": "publish_rec_cash",
        u"吸收投资收到的现金": "invest_rece_cash",
        u"收到其他与筹资活动有关的现金": "rec_other_relatecash",
        u"筹资活动现金流入小计": "borrow_cash_total",
        u"偿还债务支付的现金": "pay_debet_cash",
        u"分配股利、利润或偿付利息支付的现金": "interest_pay_cash",
        u"支付其他与筹资活动有关的现金": "pay_other_relatecash",
        u"筹资活动现金流出小计": "borrow_cash_outtotal",
        u"筹资活动产生的现金流量净额": "borrow_cash_netvalue",
        u"四、汇率变动对现金的影响": "rate_to_cash",
        u"五、现金及现金等价物净增加额": "cash_to_netadd",
        u"期初现金及现金等价物余额": "origin_cash",
        u"期末现金及现金等价物余额": "last_cash"
    }

    secrity_map = {
        u"公司统一编码": "com_uni_code",
        u"截止日期": "end_date",
        u"处置交易性金融资产净增加额": "trading_financial_dispose_netcash",
        u"收取利息、手续费及佣金的现金": "rec_interest_cash",
        u"拆入资金净增加额": "cash_netvalue",
        u"回购业务资金净增加额": "return_cash_netvalue",
        u"收到的税费返还": "tax_return",
        u"收到其他与经营活动有关的现金": "rec_other_cash",
        u"经营活动现金流入小计": "bussiness_cash_total",
        u"支付的手续费及佣金的现金": "pay_interest_cash",
        u"支付给职工以及为职工支付的现金": "pay_work_cash",
        u"支付的各项税费": "pay_tax",
        u"支付其他与经营活动有关的现金": "pay_other_cash",
        u"经营活动现金流出小计": "bussiness_cash_output",
        u"经营活动产生的现金流量净额": "bussiness_cash_net",
        u"收回投资收到的现金": "dis_invest_cash",
        u"取得投资收益收到的现金": "invest_rec_cash",
        u"收到其他与投资活动有关的现金": "rec_otherinvest_cash",
        u"投资活动现金流入小计": "invest_cash_total",
        u"投资支付的现金": "invest_pay_cash",
        u"购建固定资产、无形资产和其他长期资产支付的现金": "buy_asset_cash",
        u"支付其他与投资活动有关的现金": "pay_otherinvest_cash",
        u"投资活动现金流出小计": "invest_cash_output",
        u"投资活动产生的现金流量净额": "invest_cash_netvalue",
        u"吸收投资收到的现金": "rec_invest_reccash",
        u"取得借款收到的现金": "rec_borrow_cash",
        u"发行债券收到的现金": "publish_rec_cash",
        u"收到其他与筹资活动有关的现金": "rece_fin_cash",
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

    insurance_map = {
        u"公司统一编码": "com_uni_code",
        u"截止日期": "end_date",
        u"客户存款和同业存放款项净增加额": "custom_to_netvalue",
        u"收到原保险合同保费取得的现金": "rec_insurance_cash",
        u"收到再保业务现金净额": "rec_insurance_netvalue",
        u"保户储金及投资款净增加额": "invest_netvalue",
        u"收到的税费返还": "taxes_refu",
        u"处置交易性金融资产净增加额": "disp_trad_net_incr",
        u"收取利息、手续费及佣金的现金": "char_inte_cash",
        u"回购业务资金净增加额": "abso_rep_net_incr",
        u"收到其他与经营活动有关的现金": "rec_other_cash",
        u"经营活动现金流入小计": "bussiness_cash_total",
        u"支付原保险合同赔付款项的现金": "pay_comp_gold",
        u"客户贷款及垫款净增加额": "loans_netr",
        u"支付的手续费及佣金的现金": "pay_interest_cash",
        u"支付保单红利的现金": "pay_divi_cash",
        u"存放中央银行和同业款项净增加额": "bank_cash_netvalue",
        u"支付给职工以及为职工支付的现金": "pay_emp_cash",
        u"支付的各项税费": "pay_tax",
        u"支付其他与经营活动有关的现金": "pay_other_cash",
        u"经营活动现金流出小计": "bussiness_cash_output",
        u"经营活动产生的现金流量净额": "bussiness_cash_net",
        u"收回投资收到的现金": "rec_invest_cash",
        u"取得投资收益收到的现金": "invest_rec_cash",
        u"收到其他与投资活动有关的现金": "disp_fixed_asset_get_netc",
        u"投资活动现金流入小计": "invest_cash_total",
        u"投资支付的现金": "invest_pay_cash",
        u"质押贷款净增加额": "pled_loan_net_incr",
        u"购建固定资产、无形资产和其他长期资产支付的现金": "acqu_asset_cash",
        u"支付其他与投资活动有关的现金": "pay_otherinvest_cash",
        u"投资活动现金流出小计": "invest_cash_output",
        u"投资活动产生的现金流量净额": "invest_cash_netvalue",
        u"吸收投资收到的现金": "rec_invest_reccash",
        u"发行债券收到的现金": "publish_rec_cash",
        u"取得借款收到的现金": "rec_borrow_cash",
        u"收到其他与筹资活动有关的现金": "rece_fin_cash",
        u"筹资活动现金流入小计": "borrow_cash_total",
        u"偿还债务支付的现金": "debtpaycash  ",
        u"分配股利、利润或偿还利息支付的现金": "interest_pay_cash",
        u"支付其他与筹资活动有关的现金": "fin_rela_cash",
        u"筹资活动现金流出小计": "borrow_cash_outtotal",
        u"筹资活动产生的现金流量净额": "fin_net_cflow",
        u"四、汇率变动对现金的影响": "rate_to_cash",
        u"五、现金及现金等价物净增加额": "cash_to_netadd",
        u"期初现金及现金等价物余额": "origin_cash",
        u"期末现金及现金等价物余额": "last_cash"
    }

    cash_map = {
        u"公司统一编码": "com_uni_code",
        u"截止日期": "end_date",
        u"公告日期": "announcement_date",
        u"销售商品、提供劳务收到的现金": "sale_cash",
        u"客户存款和同业存放款项净增加额": "custom_to_netvalue",

        u"向中央银行借款净增加额": "borrow_netvalue",

        u"向其他金融机构拆入资金净增加额": "borrow_other_netvalue",
        u"代理买卖证券收到的现金净额": "trad_sec_cash_netr",
        u"收到原保险合同保费取得的现金": "rec_insurance_cash",
        u"收到再保业务现金净额": "rec_insurance_netvalue",

        u"保户储金及投资款净增加额": "invest_netvalue",

        u"收到的税费返还": "taxes_refu",
        u"处置交易性金融资产净增加额": "trading_financial_dispose_netcash",
        u"收取利息、手续费及佣金的现金": "char_inte_cash",
        u"拆入资金净增加额": "cash_netvalue",
        u"回购业务资金净增加额": "return_cash_netvalue",

        u"收到其他与经营活动有关的现金": "rec_other_cash",
        u"收到的其他与经营活动有关的现金": "rec_other_cash",

        u"经营活动现金流入小计": "bussiness_cash_total",

        u"购买商品、接受劳务支付的现金": "buy_for_cash",
        u"购买商品、提供劳务支付的现金": "buy_for_cash",

        u"客户贷款及垫款净增加额": "custom_netvalue",
        u"支付原保险合同赔付款项的现金": "pay_contra_settle_cash",

        u"支付保单红利的现金": "pay_profit_cash",

        u"支付的手续费及佣金的现金": "pay_interest_cash",

        u"存放中央银行和同业款项净增加额": "bank_cash_netvalue",
        u"支付给职工以及为职工支付的现金": "pay_emp_cash",
        u"支付的各项税费": "pay_tax",

        u"支付其他与经营活动有关的现金": "pay_other_cash",
        u"支付的其他与经营活动有关的现金": "pay_other_cash",

        u"经营活动现金流出小计": "bussiness_cash_output",
        u"经营活动产生的现金流量净额": "bussiness_cash_netvalue",

        u"收回投资收到的现金": "rec_invest_cash",
        u"收回投资所收到的现金": "rec_invest_cash",

        u"取得投资收益收到的现金": "invest_rec_cash",
        u"取得投资收益所收到的现金": "invest_rec_cash",

        u"收到其他与投资活动有关的现金": "rec_otherinvest_cash",
        u"收到的其他与投资活动有关的现金": "rec_otherinvest_cash",

        u"投资活动现金流入小计": "invest_cash_total",

        u"投资支付的现金": "invest_pay_cash",
        u"投资所支付的现金": "invest_pay_cash",

        u"质押贷款净增加额": "loan_net_addvalue",

        u"购建固定资产、无形资产和其他长期资产支付的现金": "subs_pay_cash",
        u"购建固定资产、无形资产和其他长期资产所支付的现金": "subs_pay_cash",

        u"支付其他与投资活动有关的现金": "pay_otherinvest_cash",
        u"支付的其他与投资活动有关的现金": "pay_otherinvest_cash",

        u"投资活动现金流出小计": "invest_cash_output",
        u"投资活动产生的现金流量净额": "invest_cash_netvalue",

        u"吸收投资收到的现金": "rec_invest_reccash",
        u"吸收投资所收到的现金": "rec_invest_reccash",

        u"取得借款收到的现金": "rec_borrow_cash",
        u"收到其他与筹资活动有关的现金": "rec_other_relatecash",
        u"发行债券收到的现金": "publish_rec_cash",
        u"筹资活动现金流入小计": "borrow_cash_total",

        u"偿还债务支付的现金": "pay_debet_cash",
        u"偿还债务所支付的现金": "pay_debet_cash",

        u"分配股利、利润或偿还利息支付的现金": "interest_pay_cash",
        u"分配股利、利润或偿付利息所支付的现金": "interest_pay_cash",

        u"支付其他与筹资活动有关的现金": "pay_other_relatecash",
        u"支付的其他与筹资活动有关的现金": "pay_other_relatecash",

        u"筹资活动现金流出小计": "borrow_cash_outtotal",
        u"筹资活动产生的现金流量净额": "borrow_cash_netvalue",

        u"四、汇率变动对现金及现金等价物的影响": "rate_to_cash",
        u"四、汇率变动对现金的影响": "rate_to_cash",

        u"五、现金及现金等价物净增加额": "cash_to_netadd",

        u"加:期初现金及现金等价物余额": "origin_cash",
        u"期初现金及现金等价物余额": "origin_cash",

        u"期末现金及现金等价物余额": "last_cash"

    }

    def __init__(self, conn, cursor, period, announcement):
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
        self._sess.close()
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

        global normal_num, bank_num, secrity_num, insurance_num, cash_num
        keys = "reporttype, principles, consolidation, come_source, currency_code, report_format"
        principles = 1502002
        consolidation = 1501002
        come_source = u'巨潮' + com_source_url
        currency_code = 1011001
        values = [self.report_type, principles, consolidation, come_source, currency_code]
        number = "%s, %s, %s, %s, %s, %s"

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
            if self.cash_map.get(r):
                keys = keys + "," + self.cash_map[r]
                number = number + "," + "%s"
                # remark = remark + self.normal_map[r] +','
                values.append(result[r])
        insert_sql = "insert into com_cashflow" + " (" + keys + ")" + " values (" + number + ")"
        try:
            self.cursor.execute(insert_sql, values)
            cash_num += 1
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

    def real_page(self, r):
        body = BeautifulSoup(r.text, "lxml").find("body")
        if body:
            return r
        else:
            script = BeautifulSoup(r.text, "lxml").find("script")
            if not script:
                return r

            redirect_url = self.redirect_url + script.text.split('value+\'')[1].split('\';')[0]
            r = self._sess.get(redirect_url)
            r.encoding = 'gbk'
            return r

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
            fp = open('lose_com_cash.txt', 'a')
            fp.writelines(self.account_date + com_uni_code + ' error' + '\n')
            fp.close()
            return False

    def work(self, c, proxy):
        code = c["code"]
        types = c["types"]
        dates = c["date"]
        stmt_true_plan_data = c["stmt_true_plan_data"]
        com_uni_code = c["com_uni_code"]
        # dates = c["date"].strftime("%Y-%m-%d")
        self.post_data["yyyy"] = dates[:4]
        self.post_data["mm"] = dates[4:]

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

        table_name = 'com_cashflow'
        come_source_url = "http://www.cninfo.com.cn/information/cashflow/%smb%s.html" % (types, code)
        # come_source = u'巨潮' + come_source_url

        # 增量部分：若数据库中已有记录则无需再次插入
        select_sql = 'select * from ' + table_name + ' where com_uni_code = %s and end_date = %s and come_source like %s'
        try:
            self.cursor.execute(select_sql, (result[u'公司统一编码'], result[u'截止日期'], u'巨潮%'))
            self.conn.commit()
        except Exception, e:
            logging.error(e)
            return
        res_s = self.cursor.fetchall()
        if len(res_s) > 0:
            return

        self._sess.headers.update({"Referer": "http://www.cninfo.com.cn/information/cashflow/%smb%s.html" % (types, code)})
        post_count = 1
        while True:
            try:
                r = self._sess.post(self.spider_url % (code), data=self.post_data, proxies=proxy, timeout=10)
                break
            except Exception, e:
                post_count += 1
                if post_count > 5:
                    fp = open('lose_com_cash.txt', 'a')
                    fp.writelines(self.account_date + code + ' post error' + '\n')
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
                if str1 not in r.text and str2 not in r.text and u"科目" in r.text:
                    count += 1
                    print u"repeat catch"
                    try:
                        r = self._sess.post(self.spider_url % (code), data=self.post_data, proxies=proxy, timeout=10)
                    except Exception, e:
                        continue
                    r = self.real_page(r)
                    continue
                elif str1 in r.text and str2 in r.text and u"科目" in r.text:
                    break
                elif str1 in r.text and str2 in r.text and u"科目" not in r.text:
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
                    continue
            if self.post_data['mm'] == '-06-30':
                str1 = 'selected>' + self.post_data['yyyy']
                str2 = u'selected>中期'
                if str1 not in r.text and str2 not in r.text and u"科目" in r.text:
                    print u"repeat catch"
                    count += 1
                    try:
                        r = self._sess.post(self.spider_url % (code), data=self.post_data, proxies=proxy, timeout=10)
                    except Exception, e:
                        continue
                    r = self.real_page(r)
                    continue
                elif str1 in r.text and str2 in r.text and u"科目" in r.text:
                    break
                elif str1 in r.text and str2 in r.text and u"科目" not in r.text:
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
                if str1 not in r.text and str2 not in r.text and u"科目" in r.text:
                    print u"repeat catch"
                    count += 1
                    try:
                        r = self._sess.post(self.spider_url % (code), data=self.post_data, proxies=proxy, timeout=10)
                    except Exception, e:
                        continue
                    r = self.real_page(r)
                    continue
                elif str1 in r.text and str2 in r.text and u"科目" in r.text:
                    break
                elif str1 in r.text and str2 in r.text and u"科目" not in r.text:
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
                str1 = 'selected>'+self.post_data['yyyy']
                str2 = u'selected>年度'
                if str1 not in r.text and str2 not in r.text and u"科目" in r.text:
                    print u"repeat catch"
                    count += 1
                    try:
                        r = self._sess.post(self.spider_url % (code), data=self.post_data, proxies=proxy, timeout=10)
                    except Exception, e:
                        continue
                    r = self.real_page(r)
                    continue
                elif str1 in r.text and str2 in r.text and u"科目" in r.text:
                    break
                elif str1 in r.text and str2 in r.text and u"科目" not in r.text:
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
        if care == 2:
            fp = open('lose_com_cash.txt', 'a')
            fp.writelines(self.account_date + code + 'get error\n')
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
            if line == 0:  # 忽略第一行
                line = 1
                continue
            tds = tr.find_all("td")
            for td in tds:
                datas.append(td.text)

        for i in range(0, len(datas), 2):
            if i != len(datas) - 1:
                result[datas[i].strip()] = round(self.tofloat(datas[i + 1]), 2) if datas[i+1].strip() != '' else None

        if fin_repo_type == '':
            result = {}

        if care == 2:
            result = {}
        self.insert_data(result, fin_repo_type, come_source_url)
        print code, com_uni_code, cash_num, fin_repo_type, normal_num, bank_num, secrity_num, insurance_num


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

    spider = balanceSpider(conn, cursor, end_date, today)
    spider.get_codes()
    for c in spider.codes:
        proxy = pro_use.use_proxy()
        # print proxy
        spider.work(c, proxy)
        # print c
