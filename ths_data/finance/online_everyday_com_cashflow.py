#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from mysql import mysqlUri
from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
from mailhelper import MailHelper


LOG_FILENAME = "./log_everyday_com_cashflow.txt"


class GetInfo:
    def __init__(self):
        pass

    def get_report_type(self, F014_THS015, F015_THS015):

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

    def get_date(self):
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

    def get_name_code(self):
        config_datacenter = uri.get_datacenter_mysql_uri()
        datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
        datacenter_conn = datacenter_list[0]
        datacenter_cursor = datacenter_list[1]

        '''
        获取中心库公司基本信息
        '''
        center_com_basic_sql = 'select com_uni_code,qhqm_id from com_basic_info'
        try:
            datacenter_cursor.execute(center_com_basic_sql)
        except Exception, e:
            logging.error(e)
        center_com_basic = datacenter_cursor.fetchall()
        uri.close_datacenter_MySQL(datacenter_conn, datacenter_cursor)
        center_code_list = []
        center_id_list = []
        for i in range(0, len(center_com_basic)):
            center_code_list.append(center_com_basic[i][0])
            center_id_list.append(center_com_basic[i][1])
        return [center_code_list, center_id_list]

    def get_ths002(self):
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


class EverydayComCashflow:
    def __init__(self):
        self.new_num = 0
        self.update_num = 0
        self.info = GetInfo()
        self.date_list = self.info.get_date()
        self.con_list = ['HB', 'HBTZ', 'MGS', 'MGSTZ']

        self.center_list = self.info.get_name_code()
        self.center_code_list = self.center_list[0]
        self.center_id_list = self.center_list[1]

    def get_com_uni_code(self, F001_THS015):
        try:
            com_uni_code = self.center_code_list[self.center_id_list.index(F001_THS015)]
        except Exception, e:
            com_uni_code = None
        return com_uni_code

    def insert_data(self, data):
        com_uni_code = self.get_com_uni_code(F001_THS015=data[3])
        end_date = data[6]
        report_type = self.info.get_report_type(F014_THS015=data[16], F015_THS015=data[17])

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
        values = [com_uni_code, end_date, report_type, principles, consolidation, report_format, currency_code,
                  announcement_date, data[22], data[25], data[26], data[27], data[130], data[29], data[30], data[31],
                  data[23], data[32], data[28], data[33], data[34], data[24], data[35], data[110], data[36], data[37],
                  data[41], data[44], data[43], data[46], data[45], data[132], data[42], data[38], data[39], data[40],
                  data[47], data[111], data[48], data[122], data[49], data[50], data[51], data[52], data[53], data[54],
                  data[55], data[112], data[56], data[58], data[61], data[57], data[59], data[60], data[62], data[113],
                  data[63], data[123], data[64], data[65], data[66], data[67], data[68], data[69], data[70], data[114],
                  data[71], data[72], data[73], data[74], data[75], data[76], data[115], data[77], data[124], data[78],
                  data[79], data[80], data[81], data[82], data[83], data[84], data[85], data[86], data[87], data[88],
                  data[89], data[90], data[91], data[92], data[133], data[93], data[94], data[95], data[96], data[97],
                  data[98], data[99], data[118], data[100], data[101], data[102], data[103], data[104], data[105],
                  data[106], data[107], data[125], data[126], data[108], data[109], data[119], data[120], data[121],
                  data[116], data[117], data[127], data[128], data[129], data[131]]
        insert_sql = "insert into com_cashflow" + " ( " + keys + " ) " + " values (" + number + ")"
        values.append(u'同花顺-data')

        config_datacenter = uri.get_datacenter_mysql_uri()
        datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
        datacenter_conn = datacenter_list[0]
        datacenter_cursor = datacenter_list[1]

        try:
            datacenter_cursor.execute(insert_sql, values)
            datacenter_conn.commit()
            self.new_num += 1
            print self.new_num, com_uni_code, end_date, announcement_date
        except Exception, e:
            logging.error(e)

        uri.close_datacenter_MySQL(datacenter_conn, datacenter_cursor)

    def update_data(self, new_data, center_com_cashflow):
        update_sql = 'update com_cashflow set announcement_date=%s,sale_cash=%s,custom_to_netvalue=%s,borrow_netvalue=%s,borrow_other_netvalue=%s,trad_sec_cash_netr=%s,rec_insurance_cash=%s,rec_insurance_netvalue=%s,invest_netvalue=%s,taxes_refu=%s,trading_financial_dispose_netcash=%s,char_inte_cash=%s,cash_netvalue=%s,return_cash_netvalue=%s,rec_other_cash=%s,special_items_ocif=%s,adjustment_items_ocif=%s,bussiness_cash_total=%s,buy_for_cash=%s,custom_netvalue=%s,pay_contra_settle_cash=%s,pay_interest_cash=%s,pay_profit_cash=%s,pay_com_cash=%s,trad_sec_net_decr=%s,bank_cash_netvalue=%s,pay_emp_cash=%s,pay_tax=%s,pay_other_cash=%s,special_items_ocof=%s,adjustment_items_ocof=%s,bussiness_cash_output=%s,special_bussiness_cash_net=%s,bussiness_cash_netvalue=%s,rec_invest_cash=%s,invest_rec_cash=%s,dispose_asset_netvalue=%s,subs_net_cash=%s,rec_otherinvest_cash=%s,special_items_cash=%s,adjustmen_items_cash=%s,invest_cash_total=%s,invest_pay_cash=%s,loan_net_addvalue=%s,subs_pay_cash=%s,disp_subs_pay_cash=%s,pay_otherinvest_cash=%s,special_items_icif=%s,adjustmen_items_icif=%s,invest_cash_output=%s,invest_cash_netvalue_special_item=%s,invest_cash_netvalue=%s,rec_invest_reccash=%s,cash_for_holder_invest=%s,rec_borrow_cash=%s,rec_other_relatecash=%s,publish_rec_cash=%s,special_items_fcif=%s,adjustment_items_fcif=%s,borrow_cash_total=%s,pay_debet_cash=%s,interest_pay_cash=%s,profit_for_holder=%s,pay_other_relatecash=%s,special_items_fcof=%s,adjustment_items_fcof=%s,borrow_cash_outtotal=%s,special_borrow_netcash=%s,borrow_cash_netvalue=%s,rate_to_cash=%s,cash_to_netadd=%s,origin_cash=%s,last_cash=%s,net_profit=%s,plus_asset_loss=%s,asset_depr=%s,intangible_asset_discount=%s,long_cost_discount=%s,asset_loss=%s,fix_asset_loss=%s,value_change_loss=%s,fin_cost=%s,invest_loss=%s,exch_gain_loss=%s,deferred_taxes_asset_chg=%s,deferred_taxes_liabl_chg=%s,stock_reduce=%s,rec_project_reduce=%s,pay_project_add=%s,other=%s,special_bussiness_cashnet=%s,balance_account_bussiness_cashnet=%s,indirect_management_cash_netvalue=%s,debt_to_capital=%s,debt_one_year=%s,cash_to_asset=%s,last_cash_value=%s,origin_cash_value=%s,last_cash_equiv_value=%s,origin_cash_equiv_value=%s,cash_equ_net_increase_special=%s,cash_equ_net_increase_balance=%s,indirect_cash_equiv_netvalue=%s,whether_published=%s,special_case_description=%s,paid_expenses_reduced=%s,paid_expenses_add=%s,net_increase_in_special=%s,net_increase_in_balance=%s,end_period_special=%s,end_period_balance=%s,net_reduce_net_capital=%s,net_increase_net_capital=%s  where id =%s'
        center_id = center_com_cashflow[0][0]
        center_announcement = center_com_cashflow[0][7]
        ths_announcement = new_data[21]
        # center_rtime = center_com_balance[0][7]
        # ths_rtime = new_data[2]
        if center_announcement is None:  # 中心库公告日期为空

            if ths_announcement is not None:  # 同花顺公告日期不为空才更新若为空则不变

                values = [ths_announcement,new_data[22],new_data[25],new_data[26],new_data[27],new_data[130],new_data[29],new_data[30],new_data[31],new_data[23],new_data[32],new_data[28],new_data[33],new_data[34],new_data[24],new_data[35],new_data[110],new_data[36],new_data[37],new_data[41],new_data[44],new_data[43],new_data[46],new_data[45],new_data[132],new_data[42],new_data[38],new_data[39],new_data[40],new_data[47],new_data[111],new_data[48],new_data[122],new_data[49],new_data[50],new_data[51],new_data[52],new_data[53],new_data[54],new_data[55],new_data[112],new_data[56],new_data[58],new_data[61],new_data[57],new_data[59],new_data[60],new_data[62],new_data[113],new_data[63],new_data[123],new_data[64],new_data[65],new_data[66],new_data[67],new_data[68],new_data[69],new_data[70],new_data[114],new_data[71],new_data[72],new_data[73],new_data[74],new_data[75],new_data[76],new_data[115],new_data[77],new_data[124],new_data[78],new_data[79],new_data[80],new_data[81],new_data[82],new_data[83],new_data[84],new_data[85],new_data[86],new_data[87],new_data[88],new_data[89],new_data[90],new_data[91],new_data[92],new_data[133],new_data[93],new_data[94],new_data[95],new_data[96],new_data[97],new_data[98],new_data[99],new_data[118],new_data[100],new_data[101],new_data[102],new_data[103],new_data[104],new_data[105],new_data[106],new_data[107],new_data[125],new_data[126],new_data[108],new_data[109],new_data[119],new_data[120],new_data[121],new_data[116],new_data[117],new_data[127],new_data[128],new_data[129],new_data[131],center_id]

                config_datacenter = uri.get_datacenter_mysql_uri()
                datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
                datacenter_conn = datacenter_list[0]
                datacenter_cursor = datacenter_list[1]

                try:
                    datacenter_cursor.execute(update_sql, values)
                    self.update_num += 1
                    print 'update', self.update_num
                except Exception, e:
                    logging.error(e)
                uri.close_datacenter_MySQL(datacenter_conn, datacenter_cursor)
        else:
            if center_announcement < ths_announcement:

                values = [ths_announcement, new_data[22], new_data[25], new_data[26], new_data[27], new_data[130],
                          new_data[29], new_data[30], new_data[31], new_data[23], new_data[32], new_data[28],
                          new_data[33], new_data[34], new_data[24], new_data[35], new_data[110], new_data[36],
                          new_data[37], new_data[41], new_data[44], new_data[43], new_data[46], new_data[45],
                          new_data[132], new_data[42], new_data[38], new_data[39], new_data[40], new_data[47],
                          new_data[111], new_data[48], new_data[122], new_data[49], new_data[50], new_data[51],
                          new_data[52], new_data[53], new_data[54], new_data[55], new_data[112], new_data[56],
                          new_data[58], new_data[61], new_data[57], new_data[59], new_data[60], new_data[62],
                          new_data[113], new_data[63], new_data[123], new_data[64], new_data[65], new_data[66],
                          new_data[67], new_data[68], new_data[69], new_data[70], new_data[114], new_data[71],
                          new_data[72], new_data[73], new_data[74], new_data[75], new_data[76], new_data[115],
                          new_data[77], new_data[124], new_data[78], new_data[79], new_data[80], new_data[81],
                          new_data[82], new_data[83], new_data[84], new_data[85], new_data[86], new_data[87],
                          new_data[88], new_data[89], new_data[90], new_data[91], new_data[92], new_data[133],
                          new_data[93], new_data[94], new_data[95], new_data[96], new_data[97], new_data[98],
                          new_data[99], new_data[118], new_data[100], new_data[101], new_data[102], new_data[103],
                          new_data[104], new_data[105], new_data[106], new_data[107], new_data[125], new_data[126],
                          new_data[108], new_data[109], new_data[119], new_data[120], new_data[121], new_data[116],
                          new_data[117], new_data[127], new_data[128], new_data[129], new_data[131], center_id]

                config_datacenter = uri.get_datacenter_mysql_uri()
                datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
                datacenter_conn = datacenter_list[0]
                datacenter_cursor = datacenter_list[1]

                try:
                    datacenter_cursor.execute(update_sql, values)
                    self.update_num += 1
                    print 'update', self.update_num
                except Exception, e:
                    logging.error(e)
                uri.close_datacenter_MySQL(datacenter_conn, datacenter_cursor)

    def get_new_data(self):
        today = datetime.date.today()
        five_day = datetime.timedelta(days=5)
        want_day = today - five_day
        want_day = str(want_day) + ' 00:00:00'

        ths_015_sql3 = 'SELECT * from ths015 WHERE RTIME_THS015 > %s'
        config_ths = uri.get_ths_mysql_uri()
        ths_list = uri.start_ths_MySQL(config_ths)
        ths_conn = ths_list[0]
        ths_cursor = ths_list[1]

        try:
            ths_cursor.execute(ths_015_sql3, (want_day))
        except Exception, e:
            logging.error(e)
        new_data = ths_cursor.fetchall()

        uri.close_ths_MySQL(ths_conn, ths_cursor)
        return new_data

    def get_center_have(self, new_data, com_uni_code):  # 看中心库是否有这条记录
        end_date = new_data[6]
        if new_data[7] == 'HB':
            consolidation = 1501002
        elif new_data[7] == 'MGS':
            consolidation = 1501004
        elif new_data[7] == 'HBTZ':
            consolidation = 1501001
        else:  # data[7] == 'MGSTZ':
            consolidation = 1501003

        come_source = u'同花顺-data'

        center_cashflow_sql0 = 'SELECT * from com_cashflow WHERE com_uni_code = %s and end_date = %s and consolidation = %s and come_source = %s'

        config_datacenter = uri.get_datacenter_mysql_uri()
        datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
        datacenter_conn = datacenter_list[0]
        datacenter_cursor = datacenter_list[1]

        try:
            datacenter_cursor.execute(center_cashflow_sql0, (com_uni_code, end_date, consolidation, come_source))
        except Exception, e:
            logging.error(e)
        center_com_cashflow = datacenter_cursor.fetchall()
        uri.close_datacenter_MySQL(datacenter_conn, datacenter_cursor)
        return center_com_cashflow

    def work(self):

        ths_015_0 = self.get_new_data()

        for i in range(0, len(ths_015_0)):

            new_data = ths_015_0[i]

            code0 = new_data[3]
            com_uni_code = self.get_com_uni_code(code0)
            if com_uni_code is None:
                continue

            center_com_cashflow = self.get_center_have(new_data=new_data, com_uni_code=com_uni_code)

            if len(center_com_cashflow) == 1:
                self.update_data(new_data=new_data, center_com_cashflow=center_com_cashflow)
            else:
                self.insert_data(data=new_data)

        print 'Good Job!'


class Spider:
    def __init__(self):
        pass

    def work(self):
        today = str(datetime.datetime.now())
        today = today + 'everyday_com_cashflow'

        mail_helper = MailHelper()
        try:
            mail_helper.send_mail_to(dest='ycckevinyang@163.com', message=today)
        except Exception, e:
            logging.error(e)

        everyday_com_cashflow = EverydayComCashflow()
        everyday_com_cashflow.work()

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S', filename=LOG_FILENAME, filemode='w')

    uri = mysqlUri()
    spider = Spider()

    scheduler = BlockingScheduler()
    scheduler.add_job(spider.work, 'date', run_date=datetime.datetime.now())
    scheduler.add_job(spider.work, 'cron', day_of_week='0-6', hour='9,12,15,18', minute=33, end_date='2018-08-01')

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()



