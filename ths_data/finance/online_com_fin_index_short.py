#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from mysql_cp import mysqlUri
from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
from mailhelper import MailHelper


LOG_FILENAME = "./log_com_fin_index_short.txt"


class GetInfo:
    def __init__(self):
        pass

    def get_report_type(self, F003_THS018, F002_THS018):

        report_type = 1015000
        if F003_THS018 == u'预披露公告':
            report_type = 1015015
        elif F003_THS018 == u'转让说明' or F003_THS018 == u'三板转让说明书':
            report_type = 1015013
        elif F003_THS018 == u'招股说明书(申报稿)':
            report_type = 1015017
        elif F003_THS018 == u'招股说明':
            report_type = 1015014
        elif F003_THS018 == u'招股意向书':
            report_type = 1015012
        elif F003_THS018 == u'年报':
            report_type = 1015004
        elif F003_THS018 == u'季报':
            if '03-31' in str(F002_THS018):
                report_type = 1015001
            if '09-30' in str(F002_THS018):
                report_type = 1015003
        elif F003_THS018 == u'半年报' or F003_THS018 == u'中报':
            report_type = 1015002
        elif F003_THS018 == u'上市公告书' or F003_THS018 == u'上市公告':
            report_type = 1015006
        else:
            report_type = 1015000

        return report_type

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


class EverydayComfis:
    def __init__(self):
        self.new_num = 0
        self.update_num = 0
        self.info = GetInfo()

        self.center_list = self.info.get_name_code()
        self.center_code_list = self.center_list[0]
        self.center_id_list = self.center_list[1]

    def get_com_uni_code(self, F001_THS013):
        try:
            com_uni_code = self.center_code_list[self.center_id_list.index(F001_THS013)]
        except Exception, e:
            com_uni_code = None
        return com_uni_code

    def insert_data(self, data):
        com_uni_code = self.get_com_uni_code(F001_THS013=data[3])
        end_date = data[4]
        report_type = self.info.get_report_type(data[5], data[4])

        announcement_date = data[17]

        if announcement_date is None:
            announcement_date = '0000-00-00 00:00:00'

        keys = 'com_uni_code, end_date, decl_date, report_type, total_unusual_earnings, deduct_netprofit, netassets_ps, roe_avg, roe_diluted, deduct_roe_diluted, deduct_roe_avg, ocf_ps, basic_per_income, reduce_per_income, deduct_basic_per_income, deduct_reduce_per_income, dilute_eps, deduct_dilute_eps'
        number = '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s'
        values = [com_uni_code,end_date,announcement_date,report_type,data[18], data[6], data[9], data[12], data[11], data[13], data[14], data[15], data[7], data[19], data[8], data[20], data[21], data[22]]
        insert_sql = "insert into com_fin_index_short" + " ( " + keys + " ) " + " values (" + number + ")"

        config_datacenter = uri.get_datacenter_mysql_uri()
        datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
        datacenter_conn = datacenter_list[0]
        datacenter_cursor = datacenter_list[1]

        try:
            datacenter_cursor.execute(insert_sql, values)
            datacenter_conn.commit()
            self.new_num += 1
            print 'insert', self.new_num, com_uni_code, end_date, announcement_date
        except Exception, e:
            logging.error(e)

        uri.close_datacenter_MySQL(datacenter_conn, datacenter_cursor)

    def update_data(self, new_data, center_com_balance):
        update_sql = "update com_fin_index_short set decl_date=%s, report_type=%s, total_unusual_earnings=%s, deduct_netprofit=%s, netassets_ps=%s, roe_avg=%s, roe_diluted=%s, deduct_roe_diluted=%s, deduct_roe_avg=%s, ocf_ps=%s, basic_per_income=%s, reduce_per_income=%s, deduct_basic_per_income=%s, deduct_reduce_per_income=%s, dilute_eps=%s, deduct_dilute_eps=%s where id =%s"
        center_id = center_com_balance[0][0]
        center_announcement = center_com_balance[0][3]
        ths_announcement = new_data[17]
        # center_rtime = center_com_balance[0][7]
        # ths_rtime = new_data[2]
        if center_announcement is None or center_announcement == '0000-00-00 00:00:00':  # 中心库公告日期为空

            if ths_announcement is not None:  # 同花顺公告日期不为空才更新若为空则不变

                report_type = self.info.get_report_type(new_data[5], new_data[4])
                values = [ths_announcement, report_type, new_data[18], new_data[6], new_data[9], new_data[12], new_data[11], new_data[13], new_data[14], new_data[15], new_data[7], new_data[19], new_data[8], new_data[20], new_data[21], new_data[22], center_id]

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

                report_type = self.info.get_report_type(new_data[5], new_data[4])
                values = [ths_announcement, report_type, new_data[18], new_data[6], new_data[9], new_data[12], new_data[11], new_data[13], new_data[14], new_data[15], new_data[7], new_data[19], new_data[8], new_data[20], new_data[21], new_data[22], center_id]

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

        ths_013_sql3 = 'SELECT * from ths018 WHERE RTIME_THS018 > %s'
        config_ths = uri.get_ths_mysql_uri()
        ths_list = uri.start_ths_MySQL(config_ths)
        ths_conn = ths_list[0]
        ths_cursor = ths_list[1]

        try:
            ths_cursor.execute(ths_013_sql3, (want_day))
        except Exception, e:
            logging.error(e)
        new_data = ths_cursor.fetchall()

        uri.close_ths_MySQL(ths_conn, ths_cursor)
        return new_data

    def get_center_have(self, new_data, com_uni_code):  # 看中心库是否有这条记录
        end_date = new_data[4]

        center_balance_sql0 = 'SELECT * from com_fin_index_short WHERE com_uni_code = %s and end_date = %s'

        config_datacenter = uri.get_datacenter_mysql_uri()
        datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
        datacenter_conn = datacenter_list[0]
        datacenter_cursor = datacenter_list[1]

        try:
            datacenter_cursor.execute(center_balance_sql0, (com_uni_code, end_date))
        except Exception, e:
            logging.error(e)
        center_com_balance = datacenter_cursor.fetchall()
        uri.close_datacenter_MySQL(datacenter_conn, datacenter_cursor)
        return center_com_balance

    def work(self):

        # '''
        # 获取同花顺018信息个数
        # '''
        # config_ths = uri.get_ths_mysql_uri()
        # ths_list = uri.start_ths_MySQL(config_ths)
        # ths_conn = ths_list[0]
        # ths_cursor = ths_list[1]
        #
        # ths_037_sql0 = 'select count(1) from ths018'
        #
        # ths_cursor.execute(ths_037_sql0)
        #
        # people_num = ths_cursor.fetchall()[0][0]
        #
        # uri.close_ths_MySQL(ths_conn, ths_cursor)
        #
        # limit_num = 10000
        #
        # '''
        # 获取同花顺人物表018所有信息
        # '''
        # for ii in range(0, people_num, limit_num):
        #     ths_037_sql = 'select * from ths018 limit %s, %s'
        #
        #     config_ths = uri.get_ths_mysql_uri()
        #     ths_list = uri.start_ths_MySQL(config_ths)
        #     ths_conn = ths_list[0]
        #     ths_cursor = ths_list[1]
        #
        #     ths_cursor.execute(ths_037_sql, (ii, limit_num))
        #
        #     ths_013_0 = ths_cursor.fetchall()
        #
        #     uri.close_ths_MySQL(ths_conn, ths_cursor)
        #
        #     for i in range(0, limit_num):
        #         new_data = ths_013_0[i]
        #
        #         code0 = new_data[3]
        #         com_uni_code = self.get_com_uni_code(code0)
        #         if com_uni_code is None:
        #             continue
        #
        #         center_com_balance = self.get_center_have(new_data=new_data, com_uni_code=com_uni_code)
        #
        #         if len(center_com_balance) == 1:
        #             self.update_data(new_data=new_data, center_com_balance=center_com_balance)
        #         else:
        #             self.insert_data(data=new_data)

        ths_013_0 = self.get_new_data()

        for i in range(0, len(ths_013_0)):

            new_data = ths_013_0[i]

            code0 = new_data[3]
            com_uni_code = self.get_com_uni_code(code0)
            if com_uni_code is None:
                continue

            center_com_balance = self.get_center_have(new_data=new_data, com_uni_code=com_uni_code)

            if len(center_com_balance) == 1:
                self.update_data(new_data=new_data, center_com_balance=center_com_balance)
            else:
                self.insert_data(data=new_data)

        print 'Good Job!'


class Spider:
    def __init__(self):
        pass

    def work(self):
        today = str(datetime.datetime.now())
        today = today + 'everyday_com_fin_index_short'
        mail_helper = MailHelper()
        try:
            mail_helper.send_mail_to(dest='ycckevinyang@163.com', message=today)
        except Exception, e:
            logging.error(e)
        everyday_comfis = EverydayComfis()
        everyday_comfis.work()


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S', filename=LOG_FILENAME, filemode='w')

    uri = mysqlUri()

    spider = Spider()

    scheduler = BlockingScheduler()
    scheduler.add_job(spider.work, 'date', run_date=datetime.datetime.now())
    scheduler.add_job(spider.work, 'cron', day_of_week='0-6', hour='18', minute=53, end_date='2018-08-01')

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()



