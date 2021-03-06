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

LOG_FILENAME = "./log_com_balance_sina.txt"

target_url = "http://finance.sina.com.cn/"

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11'
    }

normal_num = 0
bank_num = 0
secrity_num = 0
insurance_num = 0
balance_num = 0

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

    spider_url = "http://money.finance.sina.com.cn/corp/go.php/vFD_BalanceSheet/stockid/%s/ctrl/part/displaytype/1.phtml"

    account_date = "2017-03-31 00:00:00"

    announcement_date = "2017-03-31 00:00:00"

    normal_map = {
        u"公司统一编码": "com_uni_code",
        u"截止日期": "end_date",
        u"货币资金": "cash",
        u"交易性金融资产": "trading_fin_assets",
        u"应收票据": "rec_note",
        u"应收账款": "rec_account",
        u"预付款项": "prepay",
        u"应收利息": "rec_interest",
        u"应收股利": "rec_dividend",
        u"其他应收款": "other_rec_account",
        u"买入返售金融资产": "buy_fin_asset",
        u"存货": "stock",
        u"一年内到期的非流动资产": "non_current_asset",
        u"其他流动资产": "other_current_asset",
        u"流动资产合计": "total_current_asset",
        u"发放贷款及垫款": "loan_advance",
        u"可供出售金融资产": "available_sale_asset",
        u"持有至到期投资": "held_investment",
        u"长期应收款": "long_rec_account",
        u"长期股权投资": "long_equity_investment",
        u"投资性房地产": "invest_house",
        u"固定资产": "fix_asset",
        u"在建工程": "building",
        u"工程物资": "balance_account_asset",
        u"固定资产清理": "fix_asset_dispose",
        u"生产性生物资产": "product_asset",
        u"油气资产": "oil_asset",
        u"无形资产": "intangible_asset",
        u"开发支出": "develop_cost",
        u"商誉": "goodwill",
        u"长期待摊费用": "long_defer_cost",
        u"递延所得税资产": "tax_asset",
        u"其他非流动资产": "other_noncurrent_asset",
        u"非流动资产合计": "noncurrent_asset_total",
        u"资产总计": "asset_total",
        u"短期借款": "short_borrow",
        u"交易性金融负债": "transation_fin_borrow",
        u"应付票据": "pay_note",
        u"应付账款": "pay_account",
        u"预收款项": "prepay_account",
        u"应付手续费及佣金": "payable_fee",
        u"应付职工薪酬": "pay_salary",
        u"应交税费": "pay_tax",
        u"应付利息": "pay_interest",
        u"应付股利": "pay_dividend",
        u"其他应付款": "other_pay_account",
        u"一年内的递延收益": "non_tax_bonds",
        u"应付短期债券": "pay_short_bonds",
        u"一年内到期的非流动负债": "non_current_borrow",
        u"其他流动负债": "other_current_borrow",
        u"流动负债合计": "current_borrow_total",
        u"长期借款": "long_borrow",
        u"应付债券": "pay_bonds",
        u"长期应付款": "long_pay_account",
        u"专项应付款": "term_pay_account",
        u"预计负债": "pre_bonds",
        u"递延收益": "deferred_income",
        u"递延所得税负债": "tax_bonds",
        u"其他非流动负债": "other_noncurrent_bonds",
        u"负债合计": "bonds_total",
        u"实收资本(股本)": "rec_capital",
        u"其他权益工具": "other_equity_instrument",
        u"优先股": "prefer_stock",
        u"永续债": "sustain_debt",
        u"其他": "other",
        u"资本公积": "capital_reserve",
        u"盈余公积": "earn_reserve",
        u"减：库存股": "general_risk_preparation",
        u"其他综合收益": "other_com_income",
        u"专项储备": "special_reserve",
        u"未分配利润": "nopay_profit",
        u"归属于母公司股东权益合计": "total_account_parent_equity",
        u"少数股东权益": "monority_holder_equity",
        u"所有者权益(或股东权益)合计": "total_account_equity",
        u"负债和所有者权益(或股东权益)总计": "total_account_equity_and_liabilities"
    }

    bank_map = {
        u"公司统一编码": "com_uni_code",
        u"截止日期": "end_date",
        u"现金及存放中央银行款项": "cash_depo_cenbank",
        u"存放同业款项": "depo_other_bank",
        u"贵金属": "expensive_mental",
        u"拆出资金": "disassemble_fund",
        u"交易性金融资产": "trading_fin_assets",
        u"衍生金融工具资产": "derivat_fin_asset",
        u"买入返售金融资产": "buy_fin_asset",
        u"应收利息": "rec_interest",
        u"发放贷款及垫款": "loan_advance",
        u"可供出售金融资产": "available_sale_asset",
        u"持有至到期投资": "held_investment",
        u"应收投资款项": "rec_loan_account",
        u"长期股权投资": "long_equity_investment",
        u"投资性房地产": "invest_house",
        u"固定资产合计": "fix_asset",
        u"无形资产": "intangible_asset",
        u"商誉": "goodwill",
        u"递延税款借项": "tax_asset",
        u"其他资产": "other_asset",
        u"资产总计": "asset_total",
        u"向中央银行借款": "borrow_central",
        u"其中:同业存放款项": "peer_other_fin_depo_pay",
        u"拆入资金": "borrow_fund",
        u"交易性金融负债": "transation_fin_borrow",
        u"衍生金融工具负债": "derivat_fin_liabilities",
        u"卖出回购金融资产款": "sell_fin_asset",
        u"客户存款(吸收存款)": "absorb_depo",
        u"应付职工薪酬": "pay_salary",
        u"应交税费": "pay_tax",
        u"应付利息": "pay_interest",
        u"预计负债": "pre_bonds",
        u"递延所得税负债": "tax_bonds",
        u"应付债券": "pay_bonds",
        u"其他负债": "other_liabilities",
        u"负债合计": "bonds_total",
        u"股本": "rec_capital",
        u"其他权益工具": "other_equity_instrument",
        u"其中：优先股": "prefer_stock",
        u"资本公积": "capital_reserve",
        u"其他综合收益": "other_com_income",
        u"盈余公积": "earn_reserve",
        u"一般风险准备": "general_risk_preparation",
        u"未分配利润": "nopay_profit",
        u"外币报表折算差额": "translation_reserve",
        u"归属于母公司股东的权益": "total_account_parent_equity",
        u"少数股东权益": "monority_holder_equity",
        u"股东权益合计": "total_account_equity",
        u"负债及股东权益总计": "total_account_equity_and_liabilities"
    }

    secrity_map = {
        u"公司统一编码": "com_uni_code",
        u"截止日期": "end_date",
        u"货币资金": "cash",
        u"其中：客户资金存款": "client_deposit",
        u"结算备付金": "settlement_provision",
        u"其中：客户备付金": "client_provi",
        u"融出资金": "lend_funds",
        u"交易性金融资产": "trading_fin_assets",
        u"衍生金融资产": "derivat_fin_asset",
        u"买入返售金融资产": "buy_fin_asset",
        u"应收利息": "rec_interest",
        u"存出保证金": "refund_deposit",
        u"可供出售金融资产": "available_sale_asset",
        u"持有至到期投资": "held_investment",
        u"长期股权投资": "long_equity_investment",
        u"投资性房地产": "invest_house",
        u"固定资产": "fix_asset",
        u"在建工程": "building",
        u"无形资产": "intangible_asset",
        u"其中：交易席位费": "seat_costs",
        u"商誉": "goodwill",
        u"递延所得税资产": "tax_asset",
        u"其他资产": "other_asset",
        u"资产总计": "asset_total",
        u"短期借款": "short_borrow",
        u"其中:质押借款": "pledge_borrowing",
        u"拆入资金": "borrow_fund",
        u"交易性金融负债": "transation_fin_borrow",
        u"衍生金融负债": "derivat_fin_liabilities",
        u"卖出回购金融资产款": "sell_fin_asset",
        u"代理买卖证券款": "agent_trading_secrity",
        u"代理承销证券款": "agent_under_secrity",
        u"应付账款": "pay_account",
        u"应付职工薪酬": "pay_salary",
        u"应交税费": "pay_tax",
        u"应付利息": "pay_interest",
        u"预计负债": "pre_bonds",
        u"长期借款": "long_borrow",
        u"应付债券款": "pay_bonds",
        u"递延所得税负债": "tax_bonds",
        u"其他负债": "other_liabilities",
        u"负债合计": "bonds_total",
        u"股本": "rec_capital",
        u"其他权益工具": "other_equity_instrument",
        u"资本公积金": "capital_reserve",
        u"其他综合收益": "other_com_income",
        u"减:库存股": "treasury_stock",
        u"盈余公积金金": "earn_reserve",
        u"一般风险准备": "general_risk_preparation",
        u"交易风险准备": "trad_risk_rese",
        u"未分配利润": "nopay_profit",
        u"外币报表折算差额": "translation_reserve",
        u"归属于母公司所有者权益合计": "total_account_parent_equity",
        u"少数股东权益": "monority_holder_equity",
        u"所有者权益合计": "total_account_equity",
        u"负债及股东权益总计": "total_account_equity_and_liabilities"
    }

    insurance_map = {
        u"公司统一编码": "com_uni_code",
        u"截止日期": "end_date",
        u"货币资金": "cash",
        u"拆出资金": "disassemble_fund",
        u"交易性金融资产": "trading_fin_assets",
        u"衍生金融资产": "derivat_fin_asset",
        u"买入返售金融资产": "buy_fin_asset",
        u"应收利息": "rec_interest",
        u"应收保费": "recei_premium",
        u"应收分保账款": "recei_dividend_payment",
        u"应收分保未到期责任准备金": "recei_unearned_r",
        u"应收分保未决赔款准备金": "recei_claims_r",
        u"应收分保寿险责任准备金": "recei_life_r",
        u"应收分保长期健康险责任准备金": "recei_long_health_r",
        u"保户质押贷款": "insurer_impawn_loan",
        u"定期存款": "fixed_time_deposit",
        u"可供出售金融资产": "available_sale_asset",
        u"持有至到期投资": "held_investment",
        u"应收款项类投资": "invest_in_recei",
        u"长期股权投资": "long_equity_investment",
        u"存出资本保证金": "save_capital_deposit",
        u"投资性房地产": "invest_house",
        u"固定资产": "fix_asset",
        u"无形资产": "intangible_asset",
        u"商誉": "goodwill",
        u"递延所得税资产": "tax_asset",
        u"独立帐户资产": "independ_account_assets",
        u"其他资产": "other_asset",
        u"资产总计": "asset_total",
        u"短期借款": "short_borrow",
        u"拆入资金": "borrow_fund",
        u"交易性金融负债": "transation_fin_borrow",
        u"衍生金融负债": "derivat_fin_liabilities",
        u"卖出回购金融资产款": "sell_fin_asset",
        u"预收账款": "advance_paym",
        u"预收保费": "advance_insurance",
        u"应付手续费及佣金": "payable_fee",
        u"应付分保帐款": "pay_account_rein",
        u"应付职工薪酬": "pay_salary",
        u"应交税费": "pay_tax",
        u"应付利息": "pay_interest",
        u"预计负债": "pre_bonds",
        u"应付赔付款": "compensation_payable",
        u"应付保单红利": "policy_dividend_payable",
        u"保户储金及投资款": "insurer_deposit_investment",
        u"未到期责任准备金": "unearned_premium_reserve",
        u"未决赔款准备金": "outstanding_claim_reserve",
        u"寿险责任准备金": "life_insurance_reserve",
        u"长期健康险责任准备金": "long_health_insurance_lr",
        u"长期借款": "long_borrow",
        u"应付债券": "pay_bonds",
        u"递延所得税负债": "tax_bonds",
        u"独立帐户负债": "independ_liabilities",
        u"其他负债": "other_liabilities",
        u"负债合计": "bonds_total",
        u"股本": "rec_capital",
        u"资本公积金金": "capital_reserve",
        u"其他综合收益": "other_com_income",
        u"盈余公积金": "earn_reserve",
        u"一般风险准备": "general_risk_preparation",
        u"未分配利润": "nopay_profit",
        u"外币报表折算差额": "translation_reserve",
        u"归属于母公司的股东权益合计": "total_account_parent_equity",
        u"少数股东权益": "monority_holder_equity",
        u"所有者权益合计": "total_account_equity",
        u"负债及股东权益总计": "total_account_equity_and_liabilities"
    }

    balance_map = {
        u"公司统一编码": "com_uni_code",
        u"截止日期": "end_date",
        u"公告日期": "announcement_date",
        u"货币资金": "cash",
        u"交易性金融资产": "trading_fin_assets",
        u"应收票据": "rec_note",
        u"应收账款": "rec_account",
        u"预付款项": "prepay",
        u"应收利息": "rec_interest",
        u"应收股利": "rec_dividend",
        u"其他应收款": "other_rec_account",
        u"存货": "inventory",
        u"一年内到期的非流动资产": "non_current_asset",
        u"其他流动资产": "other_current_asset",
        u"流动资产合计": "total_current_asset",
        u"可供出售金融资产": "available_sale_asset",
        u"持有至到期投资": "held_investment",
        u"长期应收款": "long_rec_account",
        u"长期股权投资": "long_equity_investment",
        u"投资性房地产": "invest_house",

        u"固定资产": "fix_asset",
        u"固定资产净额": "fix_asset",
        u"固定资产合计": "fix_asset",

        u"在建工程": "building",
        u"工程物资": "balance_account_asset",
        u"固定资产清理": "fix_asset_dispose",
        u"生产性生物资产": "product_asset",
        u"油气资产": "oil_asset",
        u"无形资产": "intangible_asset",
        u"开发支出": "develop_cost",
        u"商誉": "goodwill",
        u"长期待摊费用": "long_defer_cost",
        u"递延所得税资产": "tax_asset",
        u"其他非流动资产": "other_noncurrent_asset",
        u"非流动资产合计": "total_noncurrent_asset",
        u"现金及存放中央银行款项": "cash_depo_cenbank",
        u"存放同业款项": "depo_other_bank",
        u"贵金属": "expensive_mental",
        u"拆出资金": "disassemble_fund",

        u"衍生金融资产": "derivat_fin_asset",
        u"衍生金融工具资产": "derivat_fin_asset",

        u"买入返售金融资产": "buy_fin_asset",
        u"发放贷款及垫款": "loan_advance",
        u"其他资产": "other_asset",
        u"应收款项类投资": "rec_loan_account",
        u"应收保费": "recei_premium",
        u"应收分保账款": "recei_dividend_payment",
        u"应收分保未到期责任准备金": "recei_unearned_r",
        u"应收分保未决赔款准备金": "recei_claims_r",
        u"应收分保寿险责任准备金": "recei_life_r",
        u"应收分保长期健康险责任准备金": "recei_long_health_r",
        u"保户质押贷款": "insurer_impawn_loan",
        u"定期存款": "fixed_time_deposit",
        u"存出资本保证金": "save_capital_deposit",
        u"独立帐户资产": "independ_account_assets",
        u"其中：客户资金存款": "customer_funds_deposit",
        u"结算备付金": "settlement_provisions",
        u"其中：客户备付金": "customer_payment",
        u"存出保证金": "refundable_deposits",
        u"其中：交易席位费": "transaction_fee",
        u"资产总计": "total_asset",
        u"短期借款": "short_borrow",
        u"交易性金融负债": "transation_fin_borrow",
        u"应付票据": "pay_note",
        u"应付账款": "pay_account",
        u"预收款项": "prepay_account",
        u"应付职工薪酬": "pay_salary",
        u"应交税费": "pay_tax",
        u"应付利息": "pay_interest",
        u"应付股利": "pay_dividend",
        u"其他应付款": "other_pay_account",
        u"一年内到期的非流动负债": "non_current_borrow",
        u"其他流动负债": "other_current_borrow",
        u"流动负债合计": "total_current_liabilities",
        u"长期借款": "long_borrow",
        u"应付债券": "pay_bonds",
        u"长期应付款": "long_pay_account",
        u"专项应付款": "term_pay_account",
        u"预计负债": "pre_bonds",
        u"递延所得税负债": "deferred_income",
        u"其他非流动负债": "other_noncurrent_bonds",
        u"非流动负债合计": "total_non_current_liabilities",
        u"向中央银行借款": "borrow_central",
        u"其中:同业存放款项": "peer_other_fin_depo_pay",
        u"拆入资金": "borrow_fund",

        u"衍生金融工具负债": "derivat_fin_liabilities",
        u"衍生金融负债": "derivat_fin_liabilities",

        u"卖出回购金融资产款": "sell_fin_asset",
        u"客户存款(吸收存款)": "absorb_depo",
        u"其他负债": "other_liabilities",
        u"预收保费": "advance_insurance",
        u"应付手续费及佣金": "payable_fee",
        u"应付分保帐款": "pay_account_rein",
        u"应付赔付款": "compensation_payable",
        u"应付保单红利": "policy_dividend_payable",
        u"保户储金及投资款": "insurer_deposit_investment",
        u"未到期责任准备金": "unearned_premium_reserve",
        u"未决赔款准备金": "outstanding_claim_reserve",
        u"寿险责任准备金": "life_insurance_reserve",
        u"长期健康险责任准备金": "long_health_insurance_lr",
        u"独立帐户负债": "independ_liabilities",
        u"其中:质押借款": "pledged_loan",
        u"代理买卖证券款": "agent_trading_secrity",
        u"代理承销证券款": "act_underwrite_securities",
        u"负债合计": "total_liabilities",

        u"实收资本(股本)": "rec_capital",
        u"股本": "rec_capital",

        u"资本公积": "capital_reserve",
        u"资本公积金": "capital_reserve",

        u"减：库存股": "treasury_stock",
        u"减:库藏股": "treasury_stock",
        u"减:库存股": "treasury_stock",

        u"专项储备": "special_reserve",
        u"其他储备": "special_reserve",

        u"盈余公积": "earn_reserve",
        u"盈余公积金金": "earn_reserve",

        u"一般风险准备": "general_normal_preparation",
        u"未分配利润": "nopay_profit",
        u"外币报表折算差额": "translation_reserve",

        u"归属于母公司股东权益合计": "total_account_parent_equity",
        u"归属于母公司股东的权益": "total_account_parent_equity",
        u"归属于母公司的股东权益合计": "total_account_parent_equity",
        u"归属于母公司所有者权益合计": "total_account_parent_equity",

        u"少数股东权益": "monority_holder_equity",

        u"所有者权益(或股东权益)合计": "total_account_equity",
        u"股东权益合计": "total_account_equity",
        u"所有者权益合计": "total_account_equity",

        u"负债和所有者权益(或股东权益)总计": "total_account_equity_and_lia",
        u"负债及股东权益总计": "total_account_equity_and_lia",

        u"应付短期债券": "short_term_bonds_payable",
        u"融出资金": "financial_capital",
        u"应付短期融资款": "short_term_financing",
        u"其他综合收益": "other_comprehensive_income",
        u"长期应付职工薪酬": "long_term_pay_for_employees",
        u"其他权益工具": "other_equity_instruments",
        u"其中：优先股": "preferred_stock"
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

        global normal_num, bank_num, secrity_num, insurance_num, balance_num
        keys = "report_type, principles, consolidation, come_source, currency_code, report_format"
        principles = 1502002
        consolidation = 1501002
        come_source = u'新浪财经' + com_source_url
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
            if self.balance_map.get(r):
                keys = keys + "," + self.balance_map[r]
                number = number + "," + "%s"
                # remark = remark + self.normal_map[r] +','
                values.append(result[r])
        insert_sql = "insert into com_balance" + " (" + keys + ")" + " values (" + number + ")"
        try:
            self.cursor.execute(insert_sql, values)
            balance_num += 1
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
            fp = open('lose_com_balance_sina.txt', 'a')
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

        table_name = 'com_balance'
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
                    fp = open('lose_com_balance_sina.txt', 'a')
                    fp.writelines(self.account_date + code + ' get error' + '\n')
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
            if line == 0: # 忽略第一行
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
