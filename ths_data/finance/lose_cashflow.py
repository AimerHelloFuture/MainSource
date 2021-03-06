#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from mysql import mysqlUri

com_codes = [3186]
ids = ['02600727']


def get_date():
    config_ths = uri.get_ths_mysql_uri()
    ths_list = uri.start_ths_MySQL(config_ths)
    ths_conn = ths_list[0]
    ths_cursor = ths_list[1]

    date_sql = 'select DISTINCT F004_THS015 from ths015 ORDER BY F004_THS015 desc'
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


def get_report_type(F014_THS015, F015_THS015):

    report_type = 1015000
    if F014_THS015 is None or F015_THS015 is None:
        return report_type

    if u'定期报告' in F014_THS015:
        if u'一季度' in F015_THS015:
            report_type = 1015001
        elif u'半年度' in F015_THS015:
            report_type = 1015002
        elif u'三季度' in F015_THS015:
            report_type = 1015003
        elif u'年度' in F015_THS015:
            report_type = 1015004
    else:
        if u'招股说明书' == F014_THS015:
            report_type = 1015014
        elif u'债券招募说明书' == F014_THS015:
            report_type = 1015005
        elif u'三板转让说明书' == F014_THS015:
            report_type = 1015013
        elif u'招股说明书申报稿' == F014_THS015:
            report_type = 1015010

    return report_type


def insert_data(com_uni_code, data):
    end_date = data[6]
    report_type = get_report_type(F014_THS015=data[16], F015_THS015=data[17])

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
    else:  # data[6] == 'MGSTZ':
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

    keys = 'com_uni_code,end_date,reporttype,principles,consolidation,report_format,currency_code,announcement_date,sale_cash,custom_to_netvalue,borrow_netvalue,borrow_other_netvalue,trad_sec_cash_netr,rec_insurance_cash,rec_insurance_netvalue,invest_netvalue,taxes_refu,trading_financial_dispose_netcash,char_inte_cash,cash_netvalue,return_cash_netvalue,rec_other_cash,special_items_ocif,adjustment_items_ocif,bussiness_cash_total,buy_for_cash,custom_netvalue,pay_contra_settle_cash,pay_interest_cash,pay_profit_cash,pay_com_cash,trad_sec_net_decr,bank_cash_netvalue,pay_emp_cash,pay_tax,pay_other_cash,special_items_ocof,adjustment_items_ocof,bussiness_cash_output,special_bussiness_cash_net,bussiness_cash_netvalue,rec_invest_cash,invest_rec_cash,dispose_asset_netvalue,subs_net_cash,rec_otherinvest_cash,special_items_cash,adjustmen_items_cash,invest_cash_total,invest_pay_cash,loan_net_addvalue,subs_pay_cash,disp_subs_pay_cash,pay_otherinvest_cash,special_items_icif,adjustmen_items_icif,invest_cash_output,invest_cash_netvalue_special_item,invest_cash_netvalue,rec_invest_reccash,cash_for_holder_invest,rec_borrow_cash,rec_other_relatecash,publish_rec_cash,special_items_fcif,adjustment_items_fcif,borrow_cash_total,pay_debet_cash,interest_pay_cash,profit_for_holder,pay_other_relatecash,special_items_fcof,adjustment_items_fcof,borrow_cash_outtotal,special_borrow_netcash,borrow_cash_netvalue,rate_to_cash,cash_to_netadd,origin_cash,last_cash,net_profit,plus_asset_loss,asset_depr,intangible_asset_discount,long_cost_discount,asset_loss,fix_asset_loss,value_change_loss,fin_cost,invest_loss,exch_gain_loss,deferred_taxes_asset_chg,deferred_taxes_liabl_chg,stock_reduce,rec_project_reduce,pay_project_add,other,special_bussiness_cashnet,balance_account_bussiness_cashnet,indirect_management_cash_netvalue,debt_to_capital,debt_one_year,cash_to_asset,last_cash_value,origin_cash_value,last_cash_equiv_value,origin_cash_equiv_value,cash_equ_net_increase_special,cash_equ_net_increase_balance,indirect_cash_equiv_netvalue,whether_published,special_case_description,paid_expenses_reduced,paid_expenses_add,net_increase_in_special,net_increase_in_balance,end_period_special,end_period_balance,net_reduce_net_capital,net_increase_net_capital,come_source'
    number = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s'
    values = [com_uni_code,end_date,report_type,principles,consolidation,report_format,currency_code,announcement_date,data[22],data[25],data[26],data[27],data[130],data[29],data[30],data[31],data[23],data[32],data[28],data[33],data[34],data[24],data[35],data[110],data[36],data[37],data[41],data[44],data[43],data[46],data[45],data[132],data[42],data[38],data[39],data[40],data[47],data[111],data[48],data[122],data[49],data[50],data[51],data[52],data[53],data[54],data[55],data[112],data[56],data[58],data[61],data[57],data[59],data[60],data[62],data[113],data[63],data[123],data[64],data[65],data[66],data[67],data[68],data[69],data[70],data[114],data[71],data[72],data[73],data[74],data[75],data[76],data[115],data[77],data[124],data[78],data[79],data[80],data[81],data[82],data[83],data[84],data[85],data[86],data[87],data[88],data[89],data[90],data[91],data[92],data[133],data[93],data[94],data[95],data[96],data[97],data[98],data[99],data[118],data[100],data[101],data[102],data[103],data[104],data[105],data[106],data[107],data[125],data[126],data[108],data[109],data[119],data[120],data[121],data[116],data[117],data[127],data[128],data[129],data[131]]
    insert_sql = "insert into com_cashflow" + " ( " + keys + " ) " + " values (" + number + ")"
    values.append(u'同花顺-data')

    config_datacenter = uri.get_datacenter_mysql_uri()
    datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
    datacenter_conn = datacenter_list[0]
    datacenter_cursor = datacenter_list[1]

    try:
        datacenter_cursor.execute(insert_sql, values)
        datacenter_conn.commit()
        print com_uni_code, end_date, announcement_date
    except Exception, e:
        logging.error(e)

    uri.close_datacenter_MySQL(datacenter_conn, datacenter_cursor)


def work():
    date_list = get_date()
    con_list = ['HB', 'HBTZ', 'MGS', 'MGSTZ']

    for m in range(0, len(ids)):
        print 'start: ' + str(m) + ' ' + ids[m]
        for i in range(0, len(date_list)):
            for j in range(0, len(con_list)):

                com_uni_code = com_codes[m]

                if con_list[j] == 'HB':
                    consolidation = 1501002
                elif con_list[j] == 'MGS':
                    consolidation = 1501004
                elif con_list[j] == 'HBTZ':
                    consolidation = 1501001
                else:  # data[6] == 'MGSTZ':
                    consolidation = 1501003

                select_sql = 'select * from com_cashflow where com_uni_code = %s and end_date = %s and consolidation = %s'

                config_datacenter = uri.get_datacenter_mysql_uri()
                datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
                datacenter_conn = datacenter_list[0]
                datacenter_cursor = datacenter_list[1]

                try:
                    datacenter_cursor.execute(select_sql, (com_uni_code, date_list[i], consolidation))
                except Exception, e:
                    logging.error(e)

                result = datacenter_cursor.fetchall()

                uri.close_datacenter_MySQL(datacenter_conn, datacenter_cursor)

                if len(result) > 0:
                    continue

                config_ths = uri.get_ths_mysql_uri()
                ths_list = uri.start_ths_MySQL(config_ths)
                ths_conn = ths_list[0]
                ths_cursor = ths_list[1]

                ths_013_sql1 = 'SELECT * from ths015 WHERE F004_THS015 = %s and F005_THS015 = %s and F001_THS015 = %s ORDER BY F019_THS015 desc, RTIME_THS015 desc'

                try:
                    ths_cursor.execute(ths_013_sql1, (date_list[i], con_list[j], ids[m]))
                except Exception, e:
                    print e
                ths_013_1 = ths_cursor.fetchall()

                uri.close_ths_MySQL(ths_conn, ths_cursor)
                if len(ths_013_1) == 0:
                    continue

                insert_data(com_uni_code=com_uni_code, data=ths_013_1[0])
        print 'end: ' + str(m) + ' ' + ids[m]


if __name__ == '__main__':

    uri = mysqlUri()

    work()

