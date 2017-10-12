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

LOG_FILENAME = "./log_cash_sina.txt"

target_url = "http://finance.sina.com.cn/"

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11'
    }

normal_num = 0
bank_num = 0
secrity_num = 0
insurance_num = 0

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

    spider_url = "http://money.finance.sina.com.cn/corp/go.php/vFD_CashFlow/stockid/%s/ctrl/part/displaytype/1.phtml"

    account_date = "2017-03-31 00:00:00"

    normal_map = {
        u"公司统一编码": "com_uni_code",
        u"截止日期": "end_date",
        u"销售商品、提供劳务收到的现金": "sale_cash",
        u"收到的税费返还": "tax_return",
        u"收到的其他与经营活动有关的现金": "rec_other_cash",
        u"经营活动现金流入小计": "bussiness_cash_total",
        u"购买商品、接受劳务支付的现金": "buy_for_cash",
        u"支付给职工以及为职工支付的现金": "pay_emp_cash",
        u"支付的各项税费": "pay_tax",
        u"支付的其他与经营活动有关的现金": "pay_other_cash",
        u"经营活动现金流出小计": "bussiness_cash_output",
        u"经营活动产生的现金流量净额": "bussiness_cash_netvalue",
        u"收回投资所收到的现金": "rec_invest_cash",
        u"取得投资收益所收到的现金": "invest_rec_cash",
        u"处置固定资产、无形资产和其他长期资产所收回的现金净额": "dispose_asset_netvalue",
        u"处置子公司及其他营业单位收到的现金净额": "subs_net_cash",
        u"收到的其他与投资活动有关的现金": "rec_otherinvest_cash",
        u"投资活动现金流入小计": "invest_cash_total",
        u"购建固定资产、无形资产和其他长期资产所支付的现金": "buy_asset_cash",
        u"投资所支付的现金": "invest_pay_cash",
        u"取得子公司及其他营业单位支付的现金净额": "subs_pay_netcash",
        u"支付的其他与投资活动有关的现金": "pay_otherinvest_cash",
        u"投资活动现金流出小计": "invest_cash_output",
        u"投资活动产生的现金流量净额": "invest_cash_netvalue",
        u"吸收投资收到的现金": "rec_invest_reccash",
        u"其中：子公司吸收少数股东投资收到的现金": "cash_for_holder_invest",
        u"取得借款收到的现金": "rec_borrow_cash",
        u"发行债券收到的现金": "publish_rec_cash",
        u"收到其他与筹资活动有关的现金": "rec_other_relatecash",
        u"筹资活动现金流入小计": "borrow_cash_total",
        u"偿还债务支付的现金": "pay_debet_cash",
        u"分配股利、利润或偿付利息所支付的现金": "interest_pay_cash",
        u"其中：子公司支付给少数股东的股利、利润": "profit_for_holder",
        u"支付其他与筹资活动有关的现金": "pay_other_relatecash",
        u"筹资活动现金流出小计": "borrow_cash_outtotal",
        u"筹资活动产生的现金流量净额": "borrow_cash_netvalue",
        u"四、汇率变动对现金及现金等价物的影响": "rate_to_cash",
        u"五、现金及现金等价物净增加额": "cash_to_netadd",
        u"加:期初现金及现金等价物余额": "origin_cash",
        u"六、期末现金及现金等价物余额": "last_cash",
        u"净利润": "net_profit",
        u"资产减值准备": "plus_asset_loss",
        u"固定资产折旧、油气资产折耗、生产性物资折旧": "asset_depr",
        u"无形资产摊销": "intangible_asset_discount",
        u"长期待摊费用摊销": "long_cost_discount",
        u"待摊费用的减少": "defer_cost_reduce",
        u"预提费用的增加": "accrued_expenses_increase",
        u"处置固定资产、无形资产和其他长期资产的损失": "asset_loss",
        u"固定资产报废损失": "fix_asset_loss",
        u"公允价值变动损失": "value_change_loss",
        u"财务费用": "fin_cost",
        u"投资损失": "invest_loss",
        u"递延收益增加（减：减少）": "deferred_taxes",
        u"递延所得税资产减少": "deferred_taxes_asset_chg",
        u"递延所得税负债增加": "deferred_taxes_liabl_chg",
        u"预计负债": "esti_debts_incr",
        u"存货的减少": "stock_reduce",
        u"经营性应收项目的减少": "rec_project_reduce",
        u"经营性应付项目的增加": "pay_project_add",
        u"其他": "other",
        u"经营活动产生现金流量净额": "indirect_management_cash_netvalue",
        u"债务转为资本": "debt_to_capital",
        u"一年内到期的可转换公司债券": "debt_one_year",
        u"融资租入固定资产": "cash_to_asset",
        u"现金的期末余额": "last_cash_value",
        u"现金的期初余额": "origin_cash_value",
        u"现金等价物的期末余额": "last_cash_equiv_value",
        u"现金等价物的期初余额": "origin_cash_equiv_value",
        u"现金及现金等价物的净增加额": "indirect_cash_equiv_netvalue"
    }

    bank_map = {
        u"公司统一编码": "com_uni_code",
        u"截止日期": "end_date",
        u"客户存款和同业存放款项净增加额": "depo_net_r",
        u"向央行借款净增加额": "cen_bank_borr_net_incr",
        u"收取利息、手续费及佣金的现金": "char_inte_cash",
        u"收到其他与经营活动有关的现金": "other_bizget_cash",
        u"经营活动现金流入小计": "bussiness_cash_total",
        u"客户贷款及垫款净增加额": "custom_netvalue",
        u"存放中央银行和同业款项净增加额": "trade_paym_incr",
        u"支付利息、手续费及佣金的现金": "pay_interest",
        u"支付给职工以及为职工支付的现金": "pay_emp_cash",
        u"支付的各项税费": "pay_tax",
        u"支付其他与经营活动有关的现金": "pay_other_biz_cash",
        u"经营活动现金流出小计": "bussiness_cash_output",
        u"经营活动产生的现金流量净额": "bussiness_cash_netvalue",
        u"收回投资收到的现金": "rec_invest_cash",
        u"取得投资收益收到的现金": "invest_rec_cash",
        u"处置固定资产、无形资产及其他资产而收到的现金": "disp_fixed_asset_get_netc",
        u"收到其他与投资活动有关的现金": "rece_other_invest_cash",
        u"投资活动现金流入小计": "invest_cash_total",
        u"投资支付的现金": "invest_pay_cash",
        u"购建固定资产、无形资产和其他长期资产支付的现金": "buy_asset_cash",
        u"支付的其他与投资活动有关的现金": "pay_inve_cash",
        u"投资活动现金流出小计": "invest_cash_output",
        u"投资活动产生的现金流量净额": "invest_cash_netvalue",
        u"发行债券收到的现金": "publish_rec_cash",
        u"吸收投资所收到的现金": "invest_rece_cash",
        u"增加股本所收到的现金": "incr_equi_get_cash",
        u"收到其他与筹资活动有关的现金": "rec_other_relatecash",
        u"筹资活动现金流入小计": "borrow_cash_total",
        u"偿还债务所支付的现金": "pay_debet_cash",
        u"分配股利、利润或偿付利息支付的现金": "interest_pay_cash",
        u"支付其他与筹资活动有关的现金": "pay_other_relatecash",
        u"筹资活动现金流出小计": "borrow_cash_outtotal",
        u"筹资活动产生的现金流量净额": "borrow_cash_netvalue",
        u"四、汇率变动对现金及现金等价物的影响": "rate_to_cash",
        u"五、现金及现金等价物净增加额": "cash_to_netadd",
        u"加:期初现金及现金等价物余额": "origin_cash",
        u"六、期末现金及现金等价物余额": "last_cash",
        u"净利润": "net_profit",
        u"计提的资产减值准备": "plus_asset_loss",
        u"计提的贷款损失准备": "loan_impa_rese",
        u"无形资产、递延资产及其他资产的摊销": "amor_asset",
        u"其中:无形资产摊销": "amorinta_asset",
        u"长期待摊费用摊销": "long_defe_expen_amor",
        u"长期资产摊销": "long_term_asset_amor",
        u"处置固定资产、无形资产和其他长期产的损失/(收益)": "asset_loss",
        u"固定资产报废损失": "fix_asset_loss",
        u"公允价值变动(收益)/损失": "value_change_loss",
        u"收到已核销款项": "rece_unco_fund",
        u"投资损失(减:收益)": "invest_loss",
        u"汇兑损益": "exch_gain_loss",
        u"递延所得税资产的减少": "defe_tax_asset_chg",
        u"递延所得税负债的增加": "defe_tax_liab_chg",
        u"存货的减少": "stock_reduce",
        u"贷款的减少": "loan_redu",
        u"存款的增加": "depo_incr",
        u"拆借款项的净增": "bank_lend_net_incr",
        u"金融性资产的减少": "fin_asset_redu",
        u"经营性应付项目的增加": "pay_project_add",
        u"其他": "other",
        u"经营活动现金流量净额": "indirect_management_cash_netvalue",
        u"债务转为资本": "debt_to_capital",
        u"一年内到期的可转换公司债券": "debt_one_year",
        u"融资租入固定资产": "cash_to_asset",
        u"其他不涉及现金收支的投资和筹资活动金额": "other_non_cash_charge_investments",
        u"现金的期末余额": "last_cash_value",
        u"减:现金的期初余额": "origin_cash_value",
        u"现金等价物的期末余额": "last_cash_equiv_value",
        u"减：现金等价物的期初余额": "origin_cash_equiv_value",
        u"现金及现金等价物净增加额": "cash_equiv_netvalue"
    }

    secrity_map = {
        u"公司统一编码": "com_uni_code",
        u"截止日期": "end_date",
        u"处置交易性金融资产净增加额": "trading_financial_dispose_netcash",
        u"处置可供出售金融资产净增加额": "fina_trad_netg",
        u"收取利息、手续费及佣金的现金": "rec_interest_cash",
        u"拆入资金净增加额": "cash_netvalue",
        u"回购业务资金净增加额": "return_cash_netvalue",
        u"收到的其他与经营活动有关的现金": "rec_other_cash",
        u"经营活动现金流入小计": "bussiness_cash_total",
        u"支付利息、手续费及佣金的现金": "pay_interest_cash",
        u"支付给职工以及为职工支付的现金": "pay_work_cash",
        u"支付的各项税费": "pay_tax",
        u"支付其他与经营活动有关的现金": "pay_other_cash",
        u"经营活动现金流出小计": "bussiness_cash_output",
        u"经营活动产生的现金流量净额": "bussiness_cash_net",
        u"收回投资收到的现金": "dis_invest_cash",
        u"取得投资收益收到的现金": "invest_rec_cash",
        u"处置固定资产、无形资产及其他长期资产收回的现金净额": "disp_fixed_get_netc",
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
        u"分配股利、利润或偿付利息所支付的现金": "interest_pay_cash",
        u"支付其他与筹资活动有关的现金": "pay_other_relatecash",
        u"筹资活动现金流出小计": "borrow_cash_outtotal",
        u"筹资活动产生的现金流量净额": "borrow_cash_netvalue",
        u"四、汇率变动对现金及现金等价物的影响": "rate_to_cash",
        u"五、现金及现金等价物净增加额": "cash_to_netadd",
        u"加:期初现金及现金等价物余额": "origin_cash",
        u"六、期末现金及现金等价物余额": "last_cash",
        u"净利润": "net_profit",
        u"资产减值准备": "plus_asset_loss",
        u"固定资产折旧、油气资产折耗、生产性生物资产折旧": "asset_depr",
        u"无形资产、递延资产及其他资产摊销": "amor_asset",
        u"其中:无形资产摊销": "amor_inta_asset",
        u"长期待摊费用摊销": "long_defe_expen_amor",
        u"长期资产摊销": "long_term_asset_amor",
        u"处置固定资产、无形资产和其他长期资产的损失": "asset_loss",
        u"固定资产报废损失": "fix_asset_loss",
        u"公允价值变动损失": "value_change_loss",
        u"财务费用": "fin_cost",
        u"投资损失": "invest_loss",
        u"汇兑损益/(损失)": "exch_gain_loss",
        u"递延所得税资产减少": "deferred_taxes_asset_chg",
        u"递延所得税负债增加": "deferred_taxes_liabl_chg",
        u"存货的减少": "stock_reduce",
        u"金融资产的减少": "trad_asset_decr",
        u"经营性应收项目的减少": "rec_project_reduce",
        u"经营性应付项目的增加": "pay_project_add",
        u"其他": "other",
        u"债务转为资本": "debt_to_capital",
        u"融资租入固定资产": "cash_to_asset",
        u"现金的期末余额": "last_cash_value",
        u"减:现金的期初余额": "origin_cash_value",
        u"加:现金等价物的期末余额": "last_cash_equiv_value",
        u"减:现金等价物的期初余额": "origin_cash_equiv_value",
        u"现金及现金等价物净增加额": "cash_equiv_netvalue"
    }

    insurance_map = {
        u"公司统一编码": "com_uni_code",
        u"截止日期": "end_date",
        u"收到原保险合同保费取得的现金": "rec_insurance_cash",
        u"收到再保业务现金净额": "rec_insurance_netvalue",
        u"保户储金净增加额": "invest_netvalue",
        u"收到其他与经营活动有关的现金": "rec_other_cash",
        u"经营活动现金流入小计": "bussiness_cash_total",
        u"支付原保险合同赔付款项的现金": "pay_comp_gold",
        u"支付手续费的现金": "pay_interest_cash",
        u"支付保单红利的现金": "pay_divi_cash",
        u"支付给职工以及为职工支付的现金": "pay_emp_cash",
        u"支付的各项税费": "pay_tax",
        u"支付其他与经营活动有关的现金": "pay_other_cash",
        u"经营活动现金流出小计": "bussiness_cash_output",
        u"经营活动产生的现金流量净额": "bussiness_cash_net",
        u"收回投资收到的现金": "rec_invest_cash",
        u"取得投资收益收到的现金": "invest_rec_cash",
        u"处置固定资产、无形资产和其他长期资产收回的现金净额": "disp_fixed_asset_get_netc",
        u"处置子公司及其他营业单位收到的现金": "disp_subs_get_cash",
        u"收到其他与投资活动有关的现金": "disp_fixed_asset_get_netc",
        u"投资活动现金流入小计": "invest_cash_total",
        u"投资支付的现金": "invest_pay_cash",
        u"质押贷款净增加额": "pled_loan_net_incr",
        u"购建固定资产、无形资产和其他长期资产支付的现金": "acqu_asset_cash",
        u"购买子公司及其他营业单位支付的现金净额": "subs_pay_cash",
        u"支付其他与投资活动有关的现金": "pay_otherinvest_cash",
        u"投资活动现金流出小计": "invest_cash_output",
        u"投资活动产生的现金流量净额": "invest_cash_netvalue",
        u"吸收投资收到的现金": "rec_invest_reccash",
        u"发行债券收到的现金": "publish_rec_cash",
        u"取得借款收到的现金": "rec_borrow_cash",
        u"收到其他与筹资活动有关的现金": "rece_fin_cash",
        u"筹资活动现金流入小计": "borrow_cash_total",
        u"偿还债务支付的现金": "debtpaycash  ",
        u"分配股利、利润或偿付利息所支付的现金": "interest_pay_cash",
        u"支付的其他与筹资活动有关的现金": "fin_rela_cash",
        u"筹资活动现金流出小计": "borrow_cash_outtotal",
        u"筹资活动产生的现金流量净额": "fin_net_cflow",
        u"四、汇率变动对现金及现金等价物的影响": "rate_to_cash",
        u"五、现金及现金等价物净增加额": "cash_to_netadd",
        u"加:期初现金及现金等价物余额": "origin_cash",
        u"六、期末现金及现金等价物余额": "last_cash",
        u"净利润": "net_profit",
        u"加:计提(转回)资产减值准备": "plus_asset_loss",
        u"计提的预计负债": "rese_esti_debts",
        u"提取的各项保险责任准备金净额": "appr_depo_duty_net",
        u"提取的未到期的责任准备金": "appr_depo_undu",
        u"投资性房地产折旧": "depreciation_fix_invest_porperty",
        u"固定资产折旧、油气资产折耗、生产性生物资产折旧": "asset_depr",
        u"无形资产、递延资产及其他资产摊销": "amor_asset",
        u"其中:无形资产摊销": "amor_int_aasset",
        u"长期待摊费用摊销": "long_defe_expen_amor",
        u"长期资产摊销": "long_term_asset_amor",
        u"处置固定资产、无形资产和其他长期资产的损失(收益)": "asset_loss",
        u"公允价值变动损失(收益)": "value_change_loss",
        u"投资收益": "invest_loss",
        u"汇兑损失(收益)": "exch_gain_loss",
        u"递延所得税费用": "deferred_taxes",
        u"其中:递延所得税资产的减少(增加)": "defe_tax_asset_chg",
        u"递延所得税负债的减少(增加)": "defe_tax_liab_chg",
        u"金融资产的减少": "trad_asset_decr",
        u"经营性应收项目的减少(增加)": "rec_project_reduce",
        u"经营性应付项目的增加(减少)": "pay_project_add",
        u"经营活动产生的现金流量净额<附表>": "bussiness_cash_netvalue",
        u"现金的期末余额": "last_cash_value",
        u"减:现金的期初余额": "origin_cash_value",
        u"加:现金等价物的期末余额": "last_cash_equiv_value",
        u"减:现金等价物的期初余额": "origin_cash_equiv_value",
        u"现金及现金等价物净增加/(减少)额": "cash_equiv_netvalue"
    }

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
            for r in res[1:]:
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

    def insert_data(self, result, fin_repo_type, com_source_url):
        global normal_num, bank_num, secrity_num, insurance_num
        keys = "report_type, principles, consolidation, come_source"
        principles = 1502002
        consolidation = 1501001
        come_source = u'新浪财经' + com_source_url
        values = [self.report_type, principles, consolidation, come_source]
        number = "%s, %s, %s, %s"
        table_name = 'cashflow_' + fin_repo_type

        # remark = '('
        if fin_repo_type == 'normal':
            for r in result.keys():
                if self.normal_map.get(r):
                    keys = keys + "," + self.normal_map[r]
                    number = number + "," + "%s"
                    # remark = remark + self.normal_map[r] +','
                    values.append(result[r])
            # remark = remark + '来自巨潮)'
            # keys = keys + ',' + 'remark'
            # number = number + "," + "%s"
            # values.append(remark)
            normal_num += 1
            insert_sql = "insert into " + table_name + " (" + keys + ")" + " values (" + number + ")"
            try:
                self.cursor.execute(insert_sql, values)
                self.conn.commit()
            except Exception, e:
                logging.error(e)

        elif fin_repo_type == 'bank':
            for r in result.keys():
                if self.bank_map.get(r):
                    keys = keys + "," + self.bank_map[r]
                    number = number + "," + "%s"
                    # remark = remark + self.normal_map[r] +','
                    values.append(result[r])
            # remark = remark + '来自巨潮)'
            # keys = keys + ',' + 'remark'
            # number = number + "," + "%s"
            # values.append(remark)
            bank_num += 1
            insert_sql = "insert into " + table_name + " (" + keys + ")" + " values (" + number + ")"
            try:
                self.cursor.execute(insert_sql, values)
                self.conn.commit()
            except Exception, e:
                logging.error(e)

        elif fin_repo_type == 'secrity':
            for r in result.keys():
                if self.secrity_map.get(r):
                    keys = keys + "," + self.secrity_map[r]
                    number = number + "," + "%s"
                    # remark = remark + self.normal_map[r] +','
                    values.append(result[r])
            # remark = remark + '来自巨潮)'
            # keys = keys + ',' + 'remark'
            # number = number + "," + "%s"
            # values.append(remark)
            secrity_num += 1
            insert_sql = "insert into " + table_name + " (" + keys + ")" + " values (" + number + ")"
            try:
                self.cursor.execute(insert_sql, values)
                self.conn.commit()
            except Exception, e:
                logging.error(e)
            pass

        else: # fin_repo_type == 'insurance'
            for r in result.keys():
                if self.insurance_map.get(r):
                    keys = keys + "," + self.insurance_map[r]
                    number = number + "," + "%s"
                    # remark = remark + self.normal_map[r] +','
                    values.append(result[r])
            # remark = remark + '来自巨潮)'
            # keys = keys + ',' + 'remark'
            # number = number + "," + "%s"
            # values.append(remark)
            insurance_num += 1
            insert_sql = "insert into " + table_name + " (" + keys + ")" + " values (" + number + ")"
            try:
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

    def search_com_type(self, code):
        try:
            search_com_sql = 'select com_uni_code from sec_basic_info where sec_code = %s'
            self.cursor.execute(search_com_sql, (code))
            res = self.cursor.fetchall()
            if len(res) < 1:
                fp = open('lose_cash_sina.txt', 'a')
                fp.writelines(self.account_date + code + ' don not have com_uni_code' + '\n')
                fp.close()
                return False
            else:
                com_uni_code = res[0][0]
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
                list = [com_uni_code, fin_repo_type]
                return list
        except Exception, e:
            fp = open('lose_cash_sina.txt', 'a')
            fp.writelines(self.account_date + code + ' don not have com_uni_code' + '\n')
            fp.close()
            return False

    def work(self, c, proxy):
        code = c["code"]
        types = c["types"]
        dates = c["date"]

        result = {}
        com_type_list = self.search_com_type(code)
        if com_type_list != False:
            com_uni_code = com_type_list[0]
            fin_repo_type = com_type_list[1]
            end_date = datetime.strptime(self.account_date, '%Y-%m-%d %H:%M:%S')
            result[u'公司统一编码'] = com_uni_code
            result[u'截止日期'] = end_date
        else:
            com_uni_code = ''
            fin_repo_type = ''
            result = {}

        table_name = 'cashflow_' + fin_repo_type
        come_source_url = self.spider_url % (code)
        come_source = u'新浪财经' + come_source_url

        # 增量部分：若数据库中已有记录则无需再次插入
        select_sql = 'select * from ' + table_name + ' where com_uni_code = %s and end_date = %s and come_source = %s'
        try:
            self.cursor.execute(select_sql, (result[u'公司统一编码'], result[u'截止日期'], come_source))
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
                    fp = open('lose_cash_sina.txt', 'a')
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
            result[datas[i].strip()] = round(self.tofloat(datas[i + 1]), 2) * 10000.0 if datas[i + 1] != '--' else None

        if com_type_list == False:
            result = {}

        self.insert_data(result, fin_repo_type, come_source_url)
        print code, com_uni_code, fin_repo_type, normal_num, bank_num, secrity_num, insurance_num
        # '''
        # 先入mongo测试
        # '''
        # result['股票信息'] = c
        # cash.insert(result)


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

    spider = balanceSpider(conn, cursor, '2017-06-30')
    spider.get_codes()
    com_uni_codes = []
    for c in spider.codes:
        select_sql1 = "select com_uni_code from sec_basic_info where sec_code = %s"
        cursor.execute(select_sql1, c["code"])
        res = cursor.fetchall()
        com_uni_code = res[0][0]
        if com_uni_code not in com_uni_codes:
            com_uni_codes.append(com_uni_code)
            proxy = pro_use.use_proxy()
            # print proxy
            spider.work(c, proxy)
            # print c
