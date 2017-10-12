# coding:utf-8
from fchc import MyPymysql


class offlinetoonline(object):
    def __init__(self):
        self.__db_dc_local = MyPymysql()
        self.__db_dc_local.connect('dc_local')
        self.__db_dc_select = MyPymysql()
        self.__db_dc_select.connect('dc_select')
        self.__db_dc_write = MyPymysql()
        self.__db_dc_write.connect('localhost')

    def __del__(self):
        self.__db_dc_write.close()
        self.__db_dc_select.close()
        self.__db_dc_local.close()

    def InsSqlGen(self):
        sql = "SELECT `com_name`,`end_date`,`main_id`,`main_id_name`,`type_name`,`type_code`," \
              "`young_estage`,`old_estage`,`emp_sum`,`rat_sum`,`come_source`FROM `com_staff_main`;"
        ins_sql = "insert into com_staff_main(`com_uni_code`, `com_name`,`end_date`,`main_id`,`main_id_name`,`type_name`,`type_code`," \
                  "`young_estage`,`old_estage`,`emp_sum`,`rat_sum`,`come_source`)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"

        last_com_name = ""
        last_com_uni_code = 0

        rs = self.__db_dc_local.run(sql)
        for r in rs:
            if (r[u'com_name'] == last_com_name):
                v1 = last_com_uni_code
            else:
                sql1 = "select com_uni_code from com_basic_info where com_name = '%s'" % r[u'com_name']
                rs1 = self.__db_dc_select.run(sql1)
                if (len(rs1) == 0):
                    print u'找不到该公司：%s' % r[u'com_name']
                    continue
                v1 = rs1[0][u'com_uni_code']
            values = (v1, r[u'com_name'], r[u'end_date'], r[u'main_id'], r[u'main_id_name'], r[u'type_name'],
                      r[u'type_code'], r[u'young_estage'], r[u'old_estage'], r[u'emp_sum'], r[u'rat_sum'],
                      r[u'come_source'])

            try:
                self.__db_dc_write.run(ins_sql, values)
            except Exception, e:
                print e
                continue

            last_com_name = r[u'com_name']
            last_com_uni_code = v1

        self.__db_dc_write.commit()


if __name__ == '__main__':

    offlinetoonline().InsSqlGen()