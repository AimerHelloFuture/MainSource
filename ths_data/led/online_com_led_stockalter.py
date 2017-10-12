#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import logging
import datetime

from mysql import mysqlUri
from apscheduler.schedulers.blocking import BlockingScheduler
from mailhelper import MailHelper


LOG_FILENAME = "./log_com_led_rewardstat.txt"


class GetInfo:
    def __init__(self):
        pass

    def get_name_code(self):
        config_datacenter = uri.get_datacenter_mysql_uri()
        datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
        datacenter_conn = datacenter_list[0]
        datacenter_cursor = datacenter_list[1]

        '''
        获取中心库证券基本信息
        '''
        center_com_basic_sql = 'select com_uni_code,sec_code from sec_basic_info'
        try:
            datacenter_cursor.execute(center_com_basic_sql)
        except Exception, e:
            logging.error(e)
        center_com_basic = datacenter_cursor.fetchall()
        uri.close_datacenter_MySQL(datacenter_conn, datacenter_cursor)
        center_com_list = []
        center_sec_list = []
        for i in range(0, len(center_com_basic)):
            center_com_list.append(center_com_basic[i][0])
            center_sec_list.append(center_com_basic[i][1])
        return [center_com_list, center_sec_list]

    def get_ths001(self):
        config_ths = uri.get_ths_mysql_uri()
        ths_list = uri.start_ths_MySQL(config_ths)
        ths_conn = ths_list[0]
        ths_cursor = ths_list[1]
        '''
        获取同花顺001信息
        '''
        ths_001_sql = 'select F002_THS001,F016_THS001 from ths001'
        try:
            ths_cursor.execute(ths_001_sql)
        except Exception, e:
            logging.error(e)
        ths001_num = ths_cursor.fetchall()
        uri.close_ths_MySQL(ths_conn, ths_cursor)
        F002_THS001_list = []
        F016_THS001_list = []
        for i in range(0, len(ths001_num)):
            F002_THS001_list.append(ths001_num[i][0])
            F016_THS001_list.append(ths001_num[i][1])
        return [F002_THS001_list, F016_THS001_list]

    def get_peonum(self):
        config_ths = uri.get_ths_mysql_uri()
        ths_list = uri.start_ths_MySQL(config_ths)
        ths_conn = ths_list[0]
        ths_cursor = ths_list[1]

        '''
        获取同花顺高管210信息个数
        '''
        ths_210_sql0 = 'select count(1) from ths210'
        try:
            ths_cursor.execute(ths_210_sql0)
        except Exception, e:
            logging.error(e)
        people_num = ths_cursor.fetchall()[0][0]
        uri.close_ths_MySQL(ths_conn, ths_cursor)
        return people_num


class ComLedRewardstat:
    def __init__(self):
        self.new_num = 0
        self.info = GetInfo()

        self.center_list = self.info.get_name_code()
        self.center_com_list = self.center_list[0]
        self.center_sec_list = self.center_list[1]

        self.ths001_list = self.info.get_ths001()
        self.F002_THS001_list = self.ths001_list[0]
        self.F016_THS001_list = self.ths001_list[1]
        self.post_map = {
            '001001': u'151000101名誉董事长',
            '001002': u'151000102董事长',
            '001003': u'151000103代理董事长',
            '001004': u'151000104副董事长',
            '001005': u'151000105执行董事',
            '001006': u'151000106独立董事',
            '001007': u'151000107董事',
            '002001': u'151000114监事会名誉主席',
            '002002': u'151000115监事长',
            '002003': u'151000116副监事长',
            '002004': u'151000117监事',
            '002005': u'151000118职工监事',
            '003004': u'151000123首席执行官',
            '003005': u'151000124总经理',
            '003006': u'151000125代理总经理',
            '003007': u'151000126常务副总经理',
            '003008': u'151000127副总经理',
            '003010': u'151000129财务总监',
            '003021': u'151000131首席技术官',
            '003025': u'151000132首席运营官',
            '003030': u'151000133市场总监',
            '003031': u'151000134首席金融业务执行官',
            '003047': u'151000135董事长助理',
            '003048': u'151000136总经理助理',
            '003049': u'151000137董事会秘书',
            '003050': u'151000138证券事务代表',
            '003051': u'151000139首席风险官',
            '003052': u'151000140合规总监',
            '003060': u'151000142高级技术人员',
            '004001': u'151000146核心技术人员',
            '005001': u'151000147顾问',
            '006001': u'151000148党委书记',
            '006002': u'151000149纪委书记',
            '006003': u'151000150党委委员',
            '006006': u'151000151组织部长',
            '006010': u'151000153工会主席',
            '003999': u'151000145高管其他',
            '006011': u'151000154副级干部',
            '003061': u'151000143代理董事会秘书',
            '003009': u'151000128代理副总经理',
            '003011': u'151000130代理财务总监',
            '003003': u'151000122代理首席执行官',
            '003062': u'151000144代理证券事务代表',
            '002006': u'151000119代理监事长',
            '006012': u'151000155代理党委书记',
            '006009': u'151000152代理工会主席',
            '001008': u'151000108董事会召集人',
            '001009': u'151000109董事会副召集人',
            '001010': u'151000110董事会临时召集人',
            '001011': u'151000111名誉董事',
            '001012': u'151000112名誉副董事长',
            '002008': u'151000120监事会副召集人',
            '002010': u'151000121监事会秘书',
            '003053': u'151000141公司秘书',
            '001013': u'151000113代理副董事长'
        }

        self.people_num = self.info.get_peonum()

    def get_com_uni_code(self, F001_THS210):
        try:
            F002_THS001 = self.F002_THS001_list[self.F016_THS001_list.index(F001_THS210)]
        except Exception, e:
            F002_THS001 = None
        try:
            com_uni_code = self.center_com_list[self.center_sec_list.index(F002_THS001)]
        except Exception, e:
            com_uni_code = None
        return com_uni_code

    def get_peo_uni_code(self, com_uni_code, F013_THS210):
            config_datacenter = uri.get_datacenter_mysql_uri()
            datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
            datacenter_conn = datacenter_list[0]
            datacenter_cursor = datacenter_list[1]

            sql1 = 'select peo_uni_code from com_led_position where com_uni_code = %s and led_name = %s'
            try:
                datacenter_cursor.execute(sql1, (com_uni_code, F013_THS210))
            except Exception, e:
                logging.error(e)
            result = datacenter_cursor.fetchall()
            uri.close_datacenter_MySQL(datacenter_conn, datacenter_cursor)

            if len(result) == 0:
                return None
            peo_uni_code = result[0][0]
            return peo_uni_code

    def work(self):

        '''
        获取同花顺高管210所有信息
        '''
        limit_num = 10000

        for i in range(0, self.people_num, limit_num):
            ths_210_sql1 = 'select * from ths210 limit %s, %s'
            config_ths = uri.get_ths_mysql_uri()
            ths_list = uri.start_ths_MySQL(config_ths)
            ths_conn = ths_list[0]
            ths_cursor = ths_list[1]
            try:
                ths_cursor.execute(ths_210_sql1, (i, limit_num))
            except Exception, e:
                logging.error(e)
            ths_210 = ths_cursor.fetchall()
            uri.close_ths_MySQL(ths_conn, ths_cursor)

            leng = len(ths_210)

            for j in range(0, leng):
                com_uni_code = self.get_com_uni_code(F001_THS210=ths_210[j][3])
                if com_uni_code is None:
                    continue
                end_date = ths_210[j][5]
                end_date = str(end_date) + ' 00:00:00'
                seq_num = ths_210[j][6]
                led_name = ths_210[j][15]

                peo_uni_code = self.get_peo_uni_code(com_uni_code=com_uni_code, F013_THS210=ths_210[j][15])
                if peo_uni_code is None:
                    fp = open('led_stockalter_lose_peounicode.txt', 'a')
                    try:
                        text = '%s %s %s %s\n' % (str(com_uni_code), end_date, str(led_name), str(ths_210[j][16]))
                        fp.write(text)
                    except Exception, e:
                        print e
                    fp.close()
                    continue

                post_code = ths_210[j][16]
                if post_code is None:
                    post_code = ''

                cha_name = ths_210[j][4]

                exec_relat = ths_210[j][14]
                if exec_relat is None:
                    exec_relat = ''

                chng_vol = ths_210[j][7]

                chng_pct = ths_210[j][12]
                if chng_pct is not None:
                    chng_pct = chng_pct / 10

                end_vol = ths_210[j][13]

                cheg_ep = ths_210[j][8]

                chan_reason = ths_210[j][11]
                if chan_reason is None:
                    chan_reason = ''

                if end_vol is None or chng_vol is None:
                    begin_vol = None
                else:
                    begin_vol = end_vol - chng_vol

                insert_sql = "insert into com_led_stockalter" + " (com_uni_code, end_date, seq_sum, peo_uni_code, led_name, post_code, cha_name, exec_relat, begin_vol, chng_vol, chng_pct, end_vol, cheg_ep, chan_reason, come_source) " + " values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                config_datacenter = uri.get_datacenter_mysql_uri()
                datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
                datacenter_conn = datacenter_list[0]
                datacenter_cursor = datacenter_list[1]

                try:
                    datacenter_cursor.execute(insert_sql, (com_uni_code, end_date, seq_num, peo_uni_code, led_name, post_code, cha_name, exec_relat, begin_vol, chng_vol, chng_pct, end_vol, cheg_ep, chan_reason, u'同花顺-data'))
                    self.new_num += 1
                    datacenter_conn.commit()
                except Exception, e:
                    logging.error(e)
                uri.close_datacenter_MySQL(datacenter_conn, datacenter_cursor)

                print self.new_num, com_uni_code, end_date, led_name


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S', filename=LOG_FILENAME, filemode='w')

    uri = mysqlUri()
    spider = ComLedRewardstat()
    spider.work()



