#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from mysql import mysqlUri
from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
from mailhelper import MailHelper


LOG_FILENAME = "./log_everyday_people_basicinfo.txt"


class GetInfo:
    def __init__(self):
        pass

    def get_country(self):
        config_ths = uri.get_ths_mysql_uri()
        ths_list = uri.start_ths_MySQL(config_ths)
        ths_conn = ths_list[0]
        ths_cursor = ths_list[1]

        '''
        获取同花顺国家表007相关国家信息
        '''
        ths_007_sql = 'select F001_THS007,F002_THS007,F003_THS007 from ths007'
        try:
            ths_cursor.execute(ths_007_sql)
        except Exception, e:
            print e
        ths_007 = ths_cursor.fetchall()

        uri.close_ths_MySQL(ths_conn, ths_cursor)

        ths_007_num = []
        ths_007_name = []
        ths_007_code = []
        for i in range(0, len(ths_007)):
            ths_007_num.append(ths_007[i][0])
            ths_007_name.append(ths_007[i][1])
            ths_007_code.append(ths_007[i][2])

        return [ths_007_num, ths_007_name, ths_007_code]


class EverydayPeopleBasic:
    def __init__(self):
        self.new_num = 0
        self.update_num = 0
        self.info = GetInfo()
        self.ths_007_list = self.info.get_country()
        self.ths_007_num = self.ths_007_list[0]
        self.ths_007_name = self.ths_007_list[1]
        self.ths_007_code = self.ths_007_list[2]

    def get_new_data(self):
        today = datetime.date.today()
        five_day = datetime.timedelta(days=5)
        want_day = today - five_day
        want_day = str(want_day) + ' 00:00:00'

        ths_037_sql3 = 'SELECT * from ths037 WHERE RTIME_THS037 > %s'
        config_ths = uri.get_ths_mysql_uri()
        ths_list = uri.start_ths_MySQL(config_ths)
        ths_conn = ths_list[0]
        ths_cursor = ths_list[1]

        try:
            ths_cursor.execute(ths_037_sql3, (want_day))
        except Exception, e:
            logging.error(e)
        new_data = ths_cursor.fetchall()

        uri.close_ths_MySQL(ths_conn, ths_cursor)
        return new_data

    def work(self):

        insert_sql = "insert into people_basicinfo " + " (people_char, name, sex_par, birth_day, country, poli_status, university, high_edu, image, back_gro, come_source) " + " values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        select_sql1 = 'select * from people_basicinfo where name = %s and sex_par = %s and birth_day = %s and country = %s and high_edu = %s'
        select_sql2 = 'select * from people_basicinfo where back_gro = %s'

        ths_037 = self.get_new_data()

        for i in range(0, len(ths_037)):

            name = ths_037[i][4]
            if name is None:
                name = ''

            if ths_037[i][5] == 'm':
                sex_par = 0
            elif ths_037[i][5] == 'f':
                sex_par = 1
            else:
                sex_par = 2
            birth_day = ths_037[i][6]

            if ths_037[i][9] is not None:
                try:
                    country = self.ths_007_code[self.ths_007_num.index(ths_037[i][9])]
                except Exception, e:
                    if u',' in ths_037[i][9]:
                        country = self.ths_007_code[self.ths_007_num.index(ths_037[i][9].split(u',')[0])]
                    else:
                        country = '100000'
            else:
                if ths_037[i][10] is not None:
                    if ths_037[i][10] in self.ths_007_name:
                        country = self.ths_007_code[self.ths_007_name.index(ths_037[i][10])]
                    else:
                        country = '100000'
                else:
                    country = '100000'

            high_edu = ths_037[i][8]
            if high_edu is None:
                high_edu = ''

            back_gro = ths_037[i][11]
            if back_gro == u'':
                back_gro = None

            config_datacenter = uri.get_datacenter_mysql_uri()
            datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
            datacenter_conn = datacenter_list[0]
            datacenter_cursor = datacenter_list[1]

            if back_gro is None:

                datacenter_cursor.execute(select_sql1, (name, sex_par, birth_day, country, high_edu))
                result1 = datacenter_cursor.fetchall()
                if len(result1) > 0:

                    peo_uni_code = result1[0][0]

                else:
                    try:
                        datacenter_cursor.execute(insert_sql, (0, name, sex_par, birth_day, country, '', '', high_edu, '', back_gro, u'同花顺-data'))
                        datacenter_conn.commit()
                        self.new_num += 1
                        print self.new_num, ths_037[i][0], name, country
                    except Exception, e:
                        logging.error(e)

            else:

                datacenter_cursor.execute(select_sql2, (back_gro))
                result2 = datacenter_cursor.fetchall()
                if len(result2) > 0:

                    peo_uni_code = result2[0][0]

                    update_sql = 'update people_basicinfo set name=%s,sex_par=%s,birth_day=%s,country=%s,high_edu=%s where peo_uni_code=%s'

                    try:
                        datacenter_cursor.execute(update_sql, (name, sex_par, birth_day, country, high_edu, peo_uni_code))
                        self.update_num += 1
                        print 'update', self.update_num
                    except Exception, e:
                        logging.error(e)

                else:
                    try:
                        datacenter_cursor.execute(insert_sql, (0, name, sex_par, birth_day, country, '', '', high_edu, '', back_gro, u'同花顺-data'))
                        datacenter_conn.commit()
                        self.new_num += 1
                        print self.new_num, ths_037[i][0], name, country
                    except Exception, e:
                        logging.error(e)

            uri.close_datacenter_MySQL(datacenter_conn, datacenter_cursor)

        print 'Good Joob!'


class Spider:
    def __init__(self):
        pass

    def work(self):
        today = str(datetime.datetime.now())
        today = today + 'everyday_people_basicinfo'

        mail_helper = MailHelper()
        try:
            mail_helper.send_mail_to(dest='ycckevinyang@163.com', message=today)
        except Exception, e:
            logging.error(e)
        everyday_people_basicinfo = EverydayPeopleBasic()
        everyday_people_basicinfo.work()


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S', filename=LOG_FILENAME, filemode='w')

    uri = mysqlUri()

    spider = Spider()
    scheduler = BlockingScheduler()
    scheduler.add_job(spider.work, 'date', run_date=datetime.datetime.now())
    scheduler.add_job(spider.work, 'cron', day_of_week='0-6', hour='8', end_date='2018-08-01')

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()














