#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from mysql import mysqlUri


new_num = 0

LOG_FILENAME = "./log_com_balance.txt"


def get_com_uni_code(F001_THS013):
    try:
        F002_THS002 = f002_ths002_list[f001_ths002_list.index(F001_THS013)]
    except Exception, e:
        F002_THS002 = None
    try:
        com_uni_code = center_code_list[center_name_list.index(F002_THS002)]
    except Exception, e:
        com_uni_code = None
    return com_uni_code


def get_report_type(F013_THS013, F014_THS013):

    report_type = 1015000
    if F013_THS013 is None or F014_THS013 is None:
        return report_type

    if u'定期报告' in F013_THS013:
        if u'一季度' in F014_THS013:
            report_type = 1015001
        elif u'半年度' in F014_THS013:
            report_type = 1015002
        elif u'三季度' in F014_THS013:
            report_type = 1015003
        elif u'年度' in F014_THS013:
            report_type = 1015004
    else:
        if u'招股说明书' == F013_THS013:
            report_type = 1015014
        elif u'债券招募说明书' == F013_THS013:
            report_type = 1015005
        elif u'三板转让说明书' == F013_THS013:
            report_type = 1015013
        elif u'招股说明书申报稿' == F013_THS013:
            report_type = 1015010

    return report_type


def insert_data(data):
    global new_num
    com_uni_code = get_com_uni_code(F001_THS013=data[3])
    end_date = data[5]
    report_type = get_report_type(F013_THS013=data[15], F014_THS013=data[16])

    principles = 1502002 if data[11] == '595001' else 1502001

    if data[18] == 'CNY':
        currency_code = 1011001
    elif data[18] == 'USD':
        currency_code = 1011002
    elif data[18] == 'EUR':
        currency_code = 1011004
    elif data[18] == 'PKR':
        currency_code = 1011008
    else:
        currency_code = 1011009

    if data[6] == 'HB':
        consolidation = 1501002
    elif data[6] == 'MGS':
        consolidation = 1501004
    elif data[6] == 'HBTZ':
        consolidation = 1501001
    else:  # data[6] == 'MGSTZ':
        consolidation = 1501003

    announcement_date = data[20]

    if data[10] == u'一般企业':
        report_format = 1013025
    elif data[10] == u'商业银行':
        report_format = 1013002
    elif data[10] == u'保险公司':
        report_format = 1013003
    else:
        report_format = 1013006

    keys = 'com_uni_code,end_date,report_type,principles,currency_code,consolidation,announcement_date,report_format,cash,trading_fin_assets,rec_note,rec_account,prepay,rec_interest,rec_dividend,other_rec_account,inventory,non_current_asset,other_current_asset,current_assets_special_subjects,current_asset_of_balance,total_current_asset,available_sale_asset,held_investment,long_rec_account,long_equity_investment,invest_house,fix_asset,building,balance_account_asset,fix_asset_dispose,product_asset,oil_asset,intangible_asset,develop_cost,goodwill,long_defer_cost,tax_asset,other_noncurrent_asset,noncurrent_asset_special_subjects,noncurrent_asset_of_balance,total_noncurrent_asset,cash_depo_cenbank,depo_other_bank,expensive_mental,disassemble_fund,derivat_fin_asset,buy_fin_asset,loan_advance,other_asset,rec_loan_account,recei_premium,receivable_subrogation,recei_dividend_payment,recei_unearned_r,recei_claims_r,recei_life_r,recei_long_health_r,insurer_impawn_loan,fixed_time_deposit,save_capital_deposit,independ_account_assets,customer_funds_deposit,settlement_provisions,customer_payment,refundable_deposits,transaction_fee,asset_special_subject,asset_balance_subject,total_asset,short_borrow,transation_fin_borrow,pay_note,pay_account,prepay_account,pay_salary,pay_tax,pay_interest,pay_dividend,other_pay_account,non_current_borrow,other_current_borrow,current_lia_special_subjects,current_lia_balance_subjects,total_current_liabilities,long_borrow,pay_bonds,long_pay_account,term_pay_account,pre_bonds,deferred_income,other_noncurrent_bonds,non_current_lia_special_subjects,non_current_lia_balance_subjects,total_non_current_liabilities,borrow_central,peer_other_fin_depo_pay,borrow_fund,derivat_fin_liabilities,sell_fin_asset,absorb_depo,other_liabilities,advance_insurance,payable_fee,pay_account_rein,compensation_payable,policy_dividend_payable,insurer_deposit_investment,unearned_premium_reserve,outstanding_claim_reserve,life_insurance_reserve,long_health_insurance_lr,independ_liabilities,pledged_loan,agent_trading_secrity,act_underwrite_securities,liabilities_special_subjects,liabilities_balance_subjects,total_liabilities,rec_capital,capital_reserve,treasury_stock,special_reserve,earn_reserve,general_normal_preparation,general_risk_preparation,nopay_profit,translation_reserve,shareholders_equity_special_subject,shareholders_equity_balance_subject,total_account_parent_equity,monority_holder_equity,total_account_equity,total_account_equity_and_lia,whether_published,special_case_description,total_number_of_shares,recei_dividend_contract,absorb_depo_and_interbank,insur_contract_reserves,deferred_income_current_lia,short_term_bonds_payable,non_current_lia_deferred_income,deposits_received,financial_capital,receivables,short_term_financing,payables,other_comprehensive_income,long_term_pay_for_employees,other_equity_instruments,preferred_stock,permanent_debt,come_source'
    number = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s'
    values = [com_uni_code,end_date,report_type,principles,currency_code,consolidation,announcement_date,report_format]
    insert_sql = "insert into com_balance" + " ( " + keys + " ) " + " values (" + number + ")"
    for value_i in range(21, 166):
        values.append(data[value_i])
    values.append(u'同花顺-data')

    config_datacenter = uri.get_datacenter_mysql_uri()
    datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
    datacenter_conn = datacenter_list[0]
    datacenter_cursor = datacenter_list[1]

    try:
        datacenter_cursor.execute(insert_sql, values)
        datacenter_conn.commit()
        new_num += 1
        print new_num, com_uni_code, end_date, announcement_date
    except Exception, e:
        logging.error(e)

    uri.close_datacenter_MySQL(datacenter_conn, datacenter_cursor)


def get_date():
    config_ths = uri.get_ths_mysql_uri()
    ths_list = uri.start_ths_MySQL(config_ths)
    ths_conn = ths_list[0]
    ths_cursor = ths_list[1]

    date_sql = 'select DISTINCT F003_THS013 from ths013 ORDER BY F003_THS013 desc'
    try:
        ths_cursor.execute(date_sql)
        # ths_conn.commit()
    except Exception, e:
        logging.error(e)
    date_num = ths_cursor.fetchall()

    uri.close_ths_MySQL(ths_conn, ths_cursor)

    date_list = []
    for i in range(0, len(date_num)):
        riqi = str(date_num[i][0])
        if '03-31' in riqi or '06-30' in riqi or '09-30' in riqi or '12-31' in riqi:
            date_list.append(date_num[i][0])
    return date_list


def get_name_code():
    config_datacenter = uri.get_datacenter_mysql_uri()
    datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
    datacenter_conn = datacenter_list[0]
    datacenter_cursor = datacenter_list[1]

    '''
    获取中心库公司基本信息
    '''
    center_com_basic_sql = 'select com_uni_code,com_name from com_basic_info'
    try:
        datacenter_cursor.execute(center_com_basic_sql)
    except Exception, e:
        logging.error(e)
    center_com_basic = datacenter_cursor.fetchall()
    uri.close_datacenter_MySQL(datacenter_conn, datacenter_cursor)
    center_name_list = []
    center_code_list = []
    for i in range(0, len(center_com_basic)):
        center_code_list.append(center_com_basic[i][0])
        center_name_list.append(center_com_basic[i][1])
    return [center_name_list, center_code_list]


def get_ths002():
    config_ths = uri.get_ths_mysql_uri()
    ths_list = uri.start_ths_MySQL(config_ths)
    ths_conn = ths_list[0]
    ths_cursor = ths_list[1]
    '''
    获取同花顺公司表002信息
    '''
    ths_002_sql = 'select f001_ths002,f002_ths002 from ths002'
    try:
        ths_cursor.execute(ths_002_sql)
        # ths_conn.commit()
    except Exception, e:
        logging.error(e)
    ths002_num = ths_cursor.fetchall()
    uri.close_ths_MySQL(ths_conn, ths_cursor)
    f001_ths002_list = []
    f002_ths002_list = []
    for i in range(0, len(ths002_num)):
        f001_ths002_list.append(ths002_num[i][0])
        f002_ths002_list.append(ths002_num[i][1])
    return [f001_ths002_list, f002_ths002_list]


def work_one(date, con, code):
    config_ths = uri.get_ths_mysql_uri()
    ths_list = uri.start_ths_MySQL(config_ths)
    ths_conn = ths_list[0]
    ths_cursor = ths_list[1]

    ths_013_sql1 = 'SELECT * from ths013 WHERE F003_THS013 = %s and F004_THS013 = %s and F001_THS013 = %s ORDER BY F018_THS013 desc, RTIME_THS013 desc '

    try:
        ths_cursor.execute(ths_013_sql1, (date, con, code))
    except Exception, e:
        logging.error(e)
    ths_013_1 = ths_cursor.fetchall()

    uri.close_ths_MySQL(ths_conn, ths_cursor)

    insert_data(data=ths_013_1[0])


def work(date_list, con_list):
    '''
    获取同花顺013所有信息
    '''
    ths_013_sql0 = 'SELECT F001_THS013,COUNT(F001_THS013) from ths013 WHERE F003_THS013 = %s and F004_THS013 = %s GROUP BY F001_THS013'
    for i in range(0, len(date_list)):
        for j in range(0, len(con_list)):

            config_ths = uri.get_ths_mysql_uri()
            ths_list = uri.start_ths_MySQL(config_ths)
            ths_conn = ths_list[0]
            ths_cursor = ths_list[1]

            try:
                ths_cursor.execute(ths_013_sql0, (date_list[i], con_list[j]))
            except Exception, e:
                logging.error(e)
            ths_013_0 = ths_cursor.fetchall()

            uri.close_ths_MySQL(ths_conn, ths_cursor)

            for k in range(0, len(ths_013_0)):
                code0 = ths_013_0[k][0]
                # num0 = ths_013_0[k][1]
                if get_com_uni_code(code0) is None:
                    continue
                work_one(date=date_list[i], con=con_list[j], code=code0)


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S', filename=LOG_FILENAME, filemode='w')

    uri = mysqlUri()

    date_list = get_date()
    con_list = ['HB', 'HBTZ', 'MGS', 'MGSTZ']

    center_list = get_name_code()
    center_name_list = center_list[0]
    center_code_list = center_list[1]

    ths002_list = get_ths002()
    f001_ths002_list = ths002_list[0]
    f002_ths002_list = ths002_list[1]

    work(date_list=date_list, con_list=con_list)


