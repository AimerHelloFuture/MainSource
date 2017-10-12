#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import datetime

from mysql import mysqlUri
from apscheduler.schedulers.blocking import BlockingScheduler
from mailhelper import MailHelper


LOG_FILENAME = "./log_com_led_rewardstat.txt"


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

    def get_peonum(self):
        config_ths = uri.get_ths_mysql_uri()
        ths_list = uri.start_ths_MySQL(config_ths)
        ths_conn = ths_list[0]
        ths_cursor = ths_list[1]

        '''
        获取同花顺高管持股及薪酬表030信息个数
        '''
        ths_030_sql0 = 'select count(1) from ths030'
        try:
            ths_cursor.execute(ths_030_sql0)
        except Exception, e:
            logging.error(e)
        people_num = ths_cursor.fetchall()[0][0]
        uri.close_ths_MySQL(ths_conn, ths_cursor)
        return people_num


class ComLedRewardstat:
    def __init__(self):
        self.new_num = 0
        self.info = GetInfo()

        self.ths_007_list = self.info.get_country()
        self.ths_007_num = self.ths_007_list[0]
        self.ths_007_name = self.ths_007_list[1]
        self.ths_007_code = self.ths_007_list[2]

        self.center_list = self.info.get_name_code()
        self.center_name_list = self.center_list[0]
        self.center_code_list = self.center_list[1]

        self.ths002_list = self.info.get_ths002()
        self.f001_ths002_list = self.ths002_list[0]
        self.f002_ths002_list = self.ths002_list[1]
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

    def get_com_uni_code(self, F001_THS030):
        try:
            F002_THS002 = self.f002_ths002_list[self.f001_ths002_list.index(F001_THS030)]
        except Exception, e:
            F002_THS002 = None
        try:
            com_uni_code = self.center_code_list[self.center_name_list.index(F002_THS002)]
        except Exception, e:
            com_uni_code = None
        return com_uni_code

    def get_peo_uni_code(self, F003_THS030):
        sql = 'select F002_THS037,F003_THS037,F004_THS037,F006_THS037,F007_THS037,F008_THS037,F009_THS037 from ths037 where F001_THS037 = %s'
        config_ths = uri.get_ths_mysql_uri()
        ths_list = uri.start_ths_MySQL(config_ths)
        ths_conn = ths_list[0]
        ths_cursor = ths_list[1]

        try:
            ths_cursor.execute(sql, F003_THS030)
        except Exception, e:
            logging.error(e)
        result = ths_cursor.fetchall()
        uri.close_ths_MySQL(ths_conn, ths_cursor)

        if len(result) == 0:
            fp = open('led_lose_peounicode.txt', 'a')
            fp.writelines(F003_THS030)
            fp.close()
            return None

        back_gro = result[0][6]

        if back_gro is not None and back_gro != u'':

            config_datacenter = uri.get_datacenter_mysql_uri()
            datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
            datacenter_conn = datacenter_list[0]
            datacenter_cursor = datacenter_list[1]

            sql1 = 'select peo_uni_code from people_basicinfo where back_gro = %s'
            try:
                datacenter_cursor.execute(sql1, back_gro)
            except Exception, e:
                logging.error(e)
            result1 = datacenter_cursor.fetchall()
            uri.close_datacenter_MySQL(datacenter_conn, datacenter_cursor)

            if len(result1) == 0:
                return None
            peo_uni_code = result1[0][0]

        else:
            name = result[0][0]
            if name is None:
                name = ''
            if result[0][1] == 'm':
                sex_par = 0
            elif result[0][1] == 'f':
                sex_par = 1
            else:
                sex_par = 2
            birth_day = result[0][2]
            high_edu = result[0][3]
            if high_edu is None:
                high_edu = ''

            country_code = result[0][4]
            country_name = result[0][5]

            if country_code is not None:
                try:
                    country = self.ths_007_code[self.ths_007_num.index(country_code)]
                except Exception, e:
                    if u',' in country_code:
                        country = self.ths_007_code[self.ths_007_num.index(country_code.split(u',')[0])]
                    else:
                        country = '100000'
            else:
                if country_name is not None:
                    if country_name in self.ths_007_name:
                        country = self.ths_007_code[self.ths_007_name.index(country_name)]
                    else:
                        country = '100000'
                else:
                    country = '100000'

            sql2 = 'select peo_uni_code from people_basicinfo where name = %s and sex_par = %s and birth_day = %s and country = %s and high_edu = %s'
            config_datacenter = uri.get_datacenter_mysql_uri()
            datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
            datacenter_conn = datacenter_list[0]
            datacenter_cursor = datacenter_list[1]

            try:
                datacenter_cursor.execute(sql2, (name, sex_par, birth_day, country, high_edu))
            except Exception, e:
                logging.error(e)
            result2 = datacenter_cursor.fetchall()

            uri.close_datacenter_MySQL(datacenter_conn, datacenter_cursor)

            if len(result2) == 0:
                return None

            peo_uni_code = result2[0][0]
        return peo_uni_code

    def get_post(self, F010_THS030, F012_THS030):

        flag1 = 0
        flag2 = 0
        post_code = []
        post_name = []
        if F010_THS030 is not None:
            list1 = F010_THS030.split(u',')
            for i in range(0, len(list1)):
                if self.post_map.get(list1[i]):
                    flag1 = 1
                    post_code.append(self.post_map[list1[i]][0:9])
                    post_name.append(self.post_map[list1[i]][9:])

        if F012_THS030 is not None:
            list2 = F012_THS030.split(u',')
            for i in range(0, len(list2)):
                if self.post_map.get(list2[i]):
                    flag2 = 1
                    post_code.append(self.post_map[list2[i]][0:9])
                    post_name.append(self.post_map[list2[i]][9:])
        if flag2 == 0 and flag1 == 0:
            return [u'其他', '151000100']
        else:
            if len(post_code) == 1:
                return [post_name[0], post_code[0]]
            else:
                code = post_code[0]
                name = post_name[0]
                for j in range(1, len(post_code)):
                    code = code + u',' + post_code[j]
                    name = name + u',' + post_name[j]
                return [name, code]

    def work(self):

        '''
        获取同花顺高管030所有信息
        '''
        limit_num = 10000

        for i in range(0, self.people_num, limit_num):
            ths_030_sql1 = 'select * from ths030 limit %s, %s'
            config_ths = uri.get_ths_mysql_uri()
            ths_list = uri.start_ths_MySQL(config_ths)
            ths_conn = ths_list[0]
            ths_cursor = ths_list[1]
            try:
                ths_cursor.execute(ths_030_sql1, (i, limit_num))
            except Exception, e:
                logging.error(e)
            ths_030 = ths_cursor.fetchall()
            uri.close_ths_MySQL(ths_conn, ths_cursor)

            leng = len(ths_030)

            for j in range(0, leng):
                com_uni_code = self.get_com_uni_code(F001_THS030=ths_030[j][3])
                if com_uni_code is None:
                    continue
                end_date = ths_030[j][4]
                decl_date = ths_030[j][16]
                if decl_date is None:
                    decl_date = '0000-00-00 00:00:00'

                if ths_030[j][5] is None:
                    continue
                peo_uni_code = self.get_peo_uni_code(F003_THS030=ths_030[j][5])
                if peo_uni_code is None:
                    continue
                led_name = ths_030[j][11]

                post_list = self.get_post(F010_THS030=ths_030[j][12], F012_THS030=ths_030[j][14])
                post_name = post_list[0]
                post_code = post_list[1]

                com_rwd = ths_030[j][9]
                if com_rwd is not None:
                    com_rwd = com_rwd / 10000

                start_shr = ths_030[j][6]
                if start_shr is not None:
                    start_shr = start_shr / 10000

                end_shr = ths_030[j][7]
                if end_shr is not None:
                    end_shr = end_shr / 10000

                if start_shr is None or end_shr is None:
                    dura_add_shr = None
                    dura_minus_shr = None
                else:
                    dura_add_shr = end_shr - start_shr
                    dura_minus_shr = start_shr - end_shr
                    if dura_add_shr < 0:
                        dura_add_shr = None
                    if dura_minus_shr < 0:
                        dura_minus_shr = None

                end_shr_rate = None
                if end_shr is not None:
                    if end_shr == 0:
                        end_shr_rate = 0
                    else:
                        share_sql = 'select total_shares from sec_struc_alter where com_uni_code = %s and chan_date = %s'
                        config_datacenter = uri.get_datacenter_mysql_uri()
                        datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
                        datacenter_conn = datacenter_list[0]
                        datacenter_cursor = datacenter_list[1]

                        try:
                            datacenter_cursor.execute(share_sql, (com_uni_code, end_date))
                        except Exception, e:
                            logging.error(e)

                        result = datacenter_cursor.fetchall()
                        uri.close_datacenter_MySQL(datacenter_conn, datacenter_cursor)
                        if len(result) != 0:
                            total_shares = result[0][0]
                            if total_shares is not None:
                                end_shr_rate = end_shr * 10000 / total_shares * 100

                chng_cause = ths_030[j][8]
                if chng_cause is None:
                    chng_cause = ''
                sal_shr_dtl = ''

                insert_sql = "insert into com_led_rewardstat" + " (com_uni_code, end_date, decl_date, peo_uni_code, led_name, post_name, Post_code, com_rwd, start_shr, dura_add_shr, dura_minus_shr, end_shr, end_shr_rate, chng_cause, sal_shr_dtl, come_source) " + " values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                config_datacenter = uri.get_datacenter_mysql_uri()
                datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
                datacenter_conn = datacenter_list[0]
                datacenter_cursor = datacenter_list[1]

                try:
                    datacenter_cursor.execute(insert_sql, (com_uni_code, end_date, decl_date, peo_uni_code, led_name, post_name, post_code, com_rwd, start_shr, dura_add_shr, dura_minus_shr,  end_shr, end_shr_rate, chng_cause, sal_shr_dtl, u'同花顺-data'))
                    self.new_num += 1
                    datacenter_conn.commit()
                except Exception, e:
                    logging.error(e)
                uri.close_datacenter_MySQL(datacenter_conn, datacenter_cursor)

                print self.new_num, com_uni_code, decl_date, led_name


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S', filename=LOG_FILENAME, filemode='w')

    uri = mysqlUri()
    spider = ComLedRewardstat()
    spider.work()



