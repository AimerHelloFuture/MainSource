#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from mysql import mysqlUri
from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
from mailhelper import MailHelper


LOG_FILENAME = "./log_everyday_com_profit.txt"


class GetInfo:
    def __init__(self):
        pass

    def get_report_type(self, F014_THS014, F015_THS014):

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

    def get_date(self):
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

    def get_name_code(self):
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


class EverydayComProfit:
    def __init__(self):
        self.new_num = 0
        self.update_num = 0
        self.info = GetInfo()
        self.date_list = self.info.get_date()
        self.con_list = ['HB', 'HBTZ', 'MGS', 'MGSTZ']

        self.center_list = self.info.get_name_code()
        self.center_name_list = self.center_list[0]
        self.center_code_list = self.center_list[1]

        self.ths002_list = self.info.get_ths002()
        self.f001_ths002_list = self.ths002_list[0]
        self.f002_ths002_list = self.ths002_list[1]

    def get_com_uni_code(self, F001_THS014):
        try:
            F002_THS002 = self.f002_ths002_list[self.f001_ths002_list.index(F001_THS014)]
        except Exception, e:
            F002_THS002 = None
        try:
            com_uni_code = self.center_code_list[self.center_name_list.index(F002_THS002)]
        except Exception, e:
            com_uni_code = None
        return com_uni_code

    def insert_data(self, data):
        com_uni_code = self.get_com_uni_code(F001_THS014=data[3])
        end_date = data[6]
        report_type = self.info.get_report_type(F014_THS014=data[16], F015_THS014=data[17])

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
        values = [com_uni_code, end_date, report_type, principles, consolidation, report_format, currency_code,
                  announcement_date, data[22], data[23], data[24], data[25], data[26], data[27], data[28], data[29],
                  data[31], data[32], data[33], data[34], data[35], data[36], data[37], data[38], data[60], data[61],
                  data[62], data[59], data[30], data[39], data[77], data[40], data[41], data[50], data[51], data[52],
                  data[53], data[54], data[55], data[56], data[42], data[48], data[57], data[43], data[44], data[45],
                  data[46], data[49], data[58], data[78], data[47], data[63], data[64], data[65], data[88], data[66],
                  data[79], data[80], data[67], data[68], data[81], data[82], data[69], data[70], data[71], data[83],
                  data[84], data[85], data[86], data[72], data[89], data[100], data[73], data[74], data[75], data[87],
                  data[90], data[91], data[92], data[101], data[93], data[94], data[95], data[96], data[97], data[98],
                  data[99], data[76]]
        insert_sql = "insert into com_profit" + " ( " + keys + " ) " + " values (" + number + ")"
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

    def update_data(self, new_data, center_com_profit):
        update_sql = 'update com_profit set announcement_date=%s,overall_income=%s,main_income=%s,interest_netincome=%s,interest_income=%s,interest_cost=%s,commission_netincome=%s,commission_income=%s,commission_cost=%s,earn_insurance=%s,insurance_bus_income=%s,premium_income=%s,separate_premiums=%s,extraction_unexpired=%s,brokerage_fee_income=%s,netincome_of_investment_banking_fees=%s,netincome_of_asset_management_fees=%s,invest_income=%s,relate_invest_income=%s,gain_loss_income=%s,value_gains=%s,other_income=%s,operating_revenue_special_course=%s,operating_revenue_balance_course=%s,overall_cost=%s,main_cost=%s,canel_insurance_money=%s,pay_expenses=%s,spread_pay_expenses=%s,insurance_liability=%s,spread_insurance_liability=%s,insurance_cost=%s,reduce_insurance_cost=%s,tax=%s,operating_manage_cost=%s,spread_premium=%s,sale_cost=%s,manage_cost=%s,fin_cost=%s,asset_loss=%s,other_bus_cost=%s,total_cost_of_special_subjects=%s,total_cost_of_balance_subjects=%s,operat_expenses=%s,overall_profit=%s,addition_income=%s,addition_cost=%s,interest_dispose=%s,disposal_noncurrent_assets=%s,Operating_profit_special_subjects=%s,Operating_profit_balance_subjects=%s,profit_total=%s,reduce_tax=%s,Profit_total_special_subjects=%s,Profit_total_balance_subjects=%s,netprofit=%s,parent_netprofit=%s,minority_loss=%s,netprofit_special_subjects=%s,netprofit_balance_subjects=%s,basic_perstock_income=%s,reduce_perstock_income=%s,other_composite_loss=%s,parent_other_com_income=%s,income_for_holer_other_com_income=%s,all_income_total=%s,income_for_parent=%s,income_for_minority=%s,special_case_description=%s,other_income_loss=%s,recalculate_change_plan=%s,equity_method=%s,others_1=%s,other_income_profit=%s,share_of_equity_method=%s,sale_financial_assets=%s,sale_financial=%s,cashflow_hedge_gains=%s,effective_portion_cashflow=%s,others_2=%s,whether_published=%s where id =%s'
        center_id = center_com_profit[0][0]
        center_announcement = center_com_profit[0][8]
        ths_announcement = new_data[21]
        # center_rtime = center_com_balance[0][7]
        # ths_rtime = new_data[2]
        if center_announcement is None:  # 中心库公告日期为空

            if ths_announcement is not None:  # 同花顺公告日期不为空才更新若为空则不变

                values = [ths_announcement, new_data[22], new_data[23], new_data[24], new_data[25], new_data[26],
                          new_data[27], new_data[28], new_data[29], new_data[31], new_data[32], new_data[33], new_data[34],
                          new_data[35], new_data[36], new_data[37], new_data[38], new_data[60], new_data[61], new_data[62],
                          new_data[59], new_data[30], new_data[39], new_data[77], new_data[40], new_data[41], new_data[50],
                          new_data[51], new_data[52], new_data[53], new_data[54], new_data[55], new_data[56], new_data[42],
                          new_data[48], new_data[57], new_data[43], new_data[44], new_data[45], new_data[46], new_data[49],
                          new_data[58], new_data[78], new_data[47], new_data[63], new_data[64], new_data[65], new_data[88],
                          new_data[66], new_data[79], new_data[80], new_data[67], new_data[68], new_data[81], new_data[82],
                          new_data[69], new_data[70], new_data[71], new_data[83], new_data[84], new_data[85], new_data[86],
                          new_data[72], new_data[89], new_data[100], new_data[73], new_data[74], new_data[75], new_data[87],
                          new_data[90], new_data[91], new_data[92], new_data[101], new_data[93], new_data[94], new_data[95],
                          new_data[96], new_data[97], new_data[98], new_data[99], new_data[76], center_id]

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

                values = [ths_announcement, new_data[22], new_data[23], new_data[24], new_data[25], new_data[26],
                          new_data[27], new_data[28], new_data[29], new_data[31], new_data[32], new_data[33],
                          new_data[34],
                          new_data[35], new_data[36], new_data[37], new_data[38], new_data[60], new_data[61],
                          new_data[62],
                          new_data[59], new_data[30], new_data[39], new_data[77], new_data[40], new_data[41],
                          new_data[50],
                          new_data[51], new_data[52], new_data[53], new_data[54], new_data[55], new_data[56],
                          new_data[42],
                          new_data[48], new_data[57], new_data[43], new_data[44], new_data[45], new_data[46],
                          new_data[49],
                          new_data[58], new_data[78], new_data[47], new_data[63], new_data[64], new_data[65],
                          new_data[88],
                          new_data[66], new_data[79], new_data[80], new_data[67], new_data[68], new_data[81],
                          new_data[82],
                          new_data[69], new_data[70], new_data[71], new_data[83], new_data[84], new_data[85],
                          new_data[86],
                          new_data[72], new_data[89], new_data[100], new_data[73], new_data[74], new_data[75],
                          new_data[87],
                          new_data[90], new_data[91], new_data[92], new_data[101], new_data[93], new_data[94],
                          new_data[95],
                          new_data[96], new_data[97], new_data[98], new_data[99], new_data[76], center_id]

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

        ths_014_sql3 = 'SELECT * from ths014 WHERE RTIME_THS014 > %s'
        config_ths = uri.get_ths_mysql_uri()
        ths_list = uri.start_ths_MySQL(config_ths)
        ths_conn = ths_list[0]
        ths_cursor = ths_list[1]

        try:
            ths_cursor.execute(ths_014_sql3, (want_day))
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

        center_profit_sql0 = 'SELECT * from com_profit WHERE com_uni_code = %s and end_date = %s and consolidation = %s and come_source = %s'

        config_datacenter = uri.get_datacenter_mysql_uri()
        datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
        datacenter_conn = datacenter_list[0]
        datacenter_cursor = datacenter_list[1]

        try:
            datacenter_cursor.execute(center_profit_sql0, (com_uni_code, end_date, consolidation, come_source))
        except Exception, e:
            logging.error(e)
        center_com_profit = datacenter_cursor.fetchall()
        uri.close_datacenter_MySQL(datacenter_conn, datacenter_cursor)
        return center_com_profit

    def work(self):

        ths_014_0 = self.get_new_data()

        for i in range(0, len(ths_014_0)):

            new_data = ths_014_0[i]

            code0 = new_data[3]
            com_uni_code = self.get_com_uni_code(code0)
            if com_uni_code is None:
                continue

            center_com_profit = self.get_center_have(new_data=new_data, com_uni_code=com_uni_code)

            if len(center_com_profit) == 1:
                self.update_data(new_data=new_data, center_com_profit=center_com_profit)
            else:
                self.insert_data(data=new_data)

        print 'Good Job!'


class Spider:
    def __init__(self):
        pass

    def work(self):
        today = str(datetime.datetime.now())
        today = today + 'everyday_com_profit'

        mail_helper = MailHelper()
        try:
            mail_helper.send_mail_to(dest='ycckevinyang@163.com', message=today)
        except Exception, e:
            logging.error(e)
        everyday_com_profit = EverydayComProfit()
        everyday_com_profit.work()

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S', filename=LOG_FILENAME, filemode='w')

    uri = mysqlUri()
    spider = Spider()

    scheduler = BlockingScheduler()
    # scheduler.add_job(spider.work, 'date', run_date=datetime.datetime.now())
    scheduler.add_job(spider.work, 'cron', day_of_week='0-6', hour='9,12,15,18', minute=23, end_date='2018-08-01')

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()



