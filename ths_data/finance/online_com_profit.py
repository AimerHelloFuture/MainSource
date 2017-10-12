#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from mysql import mysqlUri


new_num = 0

LOG_FILENAME = "./log_com_profit.txt"


def get_com_uni_code(F001_THS014):
    try:
        F002_THS002 = f002_ths002_list[f001_ths002_list.index(F001_THS014)]
    except Exception, e:
        F002_THS002 = None
    try:
        com_uni_code = center_code_list[center_name_list.index(F002_THS002)]
    except Exception, e:
        com_uni_code = None
    return com_uni_code


def get_report_type(F014_THS014, F015_THS014):

    report_type = 1015000
    if F014_THS014 is None or F015_THS014 is None:
        return report_type

    if u'定期报告' in F014_THS014:
        if u'一季度' in F015_THS014:
            report_type = 1015001
        elif u'半年度' in F015_THS014:
            report_type = 1015002
        elif u'三季度' in F015_THS014:
            report_type = 1015003
        elif u'年度' in F015_THS014:
            report_type = 1015004
    else:
        if u'招股说明书' == F014_THS014:
            report_type = 1015014
        elif u'债券招募说明书' == F014_THS014:
            report_type = 1015005
        elif u'三板转让说明书' == F014_THS014:
            report_type = 1015013
        elif u'招股说明书申报稿' == F014_THS014:
            report_type = 1015010

    return report_type


def insert_data(data):
    global new_num
    com_uni_code = get_com_uni_code(F001_THS014=data[3])
    end_date = data[6]
    report_type = get_report_type(F014_THS014=data[16], F015_THS014=data[17])

    principles = 1502002 if data[12] == '595001' else 1502001

    if data[19] == 'CNY':
        currency_code = 1011001
    elif data[19] == 'USD':
        currency_code = 1011002
    elif data[19] == 'EUR':
        currency_code = 1011004
    elif data[19] == 'PKR':
        currency_code = 1011008
    else:
        currency_code = 1011009

    if data[7] == 'HB':
        consolidation = 1501002
    elif data[7] == 'MGS':
        consolidation = 1501004
    elif data[7] == 'HBTZ':
        consolidation = 1501001
    else:  # data[7] == 'MGSTZ':
        consolidation = 1501003

    announcement_date = data[21]

    if data[11] == u'一般企业':
        report_format = 1013025
    elif data[11] == u'商业银行':
        report_format = 1013002
    elif data[11] == u'保险公司':
        report_format = 1013003
    else:
        report_format = 1013006

    keys = 'com_uni_code,end_date,report_type,principles,consolidation,report_format,currency_code,announcement_date,overall_income,main_income,interest_netincome,interest_income,interest_cost,commission_netincome,commission_income,commission_cost,earn_insurance,insurance_bus_income,premium_income,separate_premiums,extraction_unexpired,brokerage_fee_income,netincome_of_investment_banking_fees,netincome_of_asset_management_fees,invest_income,relate_invest_income,gain_loss_income,value_gains,other_income,operating_revenue_special_course,operating_revenue_balance_course,overall_cost,main_cost,canel_insurance_money,pay_expenses,spread_pay_expenses,insurance_liability,spread_insurance_liability,insurance_cost,reduce_insurance_cost,tax,operating_manage_cost,spread_premium,sale_cost,manage_cost,fin_cost,asset_loss,other_bus_cost,total_cost_of_special_subjects,total_cost_of_balance_subjects,operat_expenses,overall_profit,addition_income,addition_cost,interest_dispose,disposal_noncurrent_assets,Operating_profit_special_subjects,Operating_profit_balance_subjects,profit_total,reduce_tax,Profit_total_special_subjects,Profit_total_balance_subjects,netprofit,parent_netprofit,minority_loss,netprofit_special_subjects,netprofit_balance_subjects,basic_perstock_income,reduce_perstock_income,other_composite_loss,parent_other_com_income,income_for_holer_other_com_income,all_income_total,income_for_parent,income_for_minority,special_case_description,other_income_loss,recalculate_change_plan,equity_method,others_1,other_income_profit,share_of_equity_method,sale_financial_assets,sale_financial,cashflow_hedge_gains,effective_portion_cashflow,others_2,whether_published,come_source'
    number = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s'
    values = [com_uni_code,end_date,report_type,principles,consolidation,report_format,currency_code,announcement_date,data[22],data[23],data[24],data[25],data[26],data[27],data[28],data[29],data[31],data[32],data[33],data[34],data[35],data[36],data[37],data[38],data[60],data[61],data[62],data[59],data[30],data[39],data[77],data[40],data[41],data[50],data[51],data[52],data[53],data[54],data[55],data[56],data[42],data[48],data[57],data[43],data[44],data[45],data[46],data[49],data[58],data[78],data[47],data[63],data[64],data[65],data[88],data[66],data[79],data[80],data[67],data[68],data[81],data[82],data[69],data[70],data[71],data[83],data[84],data[85],data[86],data[72],data[89],data[100],data[73],data[74],data[75],data[87],data[90],data[91],data[92],data[101],data[93],data[94],data[95],data[96],data[97],data[98],data[99],data[76]]
    insert_sql = "insert into com_profit" + " ( " + keys + " ) " + " values (" + number + ")"
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

    date_sql = 'select DISTINCT F004_THS014 from ths014 ORDER BY F004_THS014 desc'
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

    ths_014_sql1 = 'SELECT * from ths014 WHERE F004_THS014 = %s and F005_THS014 = %s and F001_THS014 = %s ORDER BY F019_THS014 desc, RTIME_THS014 desc'

    try:
        ths_cursor.execute(ths_014_sql1, (date, con, code))
    except Exception, e:
        logging.error(e)
    ths_014_1 = ths_cursor.fetchall()

    uri.close_ths_MySQL(ths_conn, ths_cursor)

    insert_data(data=ths_014_1[0])


def work(date_list, con_list):
    '''
    获取同花顺014所有信息
    '''
    ths_014_sql0 = 'SELECT F001_THS014,COUNT(F001_THS014) from ths014 WHERE F004_THS014 = %s and F005_THS014 = %s GROUP BY F001_THS014'

    for i in range(23, len(date_list)):
        for j in range(0, len(con_list)):

            config_ths = uri.get_ths_mysql_uri()
            ths_list = uri.start_ths_MySQL(config_ths)
            ths_conn = ths_list[0]
            ths_cursor = ths_list[1]

            try:
                ths_cursor.execute(ths_014_sql0, (date_list[i], con_list[j]))
            except Exception, e:
                logging.error(e)
            ths_014_0 = ths_cursor.fetchall()

            uri.close_ths_MySQL(ths_conn, ths_cursor)

            for k in range(0, len(ths_014_0)):
                code0 = ths_014_0[k][0]
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


