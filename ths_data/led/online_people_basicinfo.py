#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mysql import mysqlUri

if __name__ == '__main__':

    new_num = 0

    uri = mysqlUri()

    config_ths = uri.get_ths_mysql_uri()
    ths_list = uri.start_ths_MySQL(config_ths)
    ths_conn = ths_list[0]
    ths_cursor = ths_list[1]

    config_datacenter = uri.get_datacenter_mysql_uri()
    datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
    datacenter_conn = datacenter_list[0]
    datacenter_cursor = datacenter_list[1]


    '''
    获取中心库国家code
    '''
    center_region_sql = 'select code from region'
    try:
        datacenter_cursor.execute(center_region_sql)
        datacenter_conn.commit()
    except Exception, e:
        print e
    center_region = datacenter_cursor.fetchall()

    center_region_list = []
    for i in range(0, len(center_region)):
        center_region_list.append(center_region[i][0])


    '''
    获取同花顺国家表007相关国家信息
    '''
    ths_007_sql = 'select F001_THS007,F002_THS007,F003_THS007 from ths007'
    try:
        ths_cursor.execute(ths_007_sql)
        ths_conn.commit()
    except Exception, e:
        print e
    ths_007 = ths_cursor.fetchall()

    ths_007_num = []
    ths_007_name = []
    ths_007_code = []
    for i in range(0, len(ths_007)):
        ths_007_num.append(ths_007[i][0])
        ths_007_name.append(ths_007[i][1])
        ths_007_code.append(ths_007[i][2])


    '''
    获取同花顺人物表037信息个数
    '''
    ths_037_sql0 = 'select count(1) from ths037'
    try:
        ths_cursor.execute(ths_037_sql0)
        ths_conn.commit()
    except Exception, e:
        print e
    people_num = ths_cursor.fetchall()[0][0]


    '''
    获取同花顺人物表037所有信息
    '''
    for i in range(0, people_num, 10000):
        ths_037_sql = 'select * from ths037 limit %s, 10000'
        try:
            ths_cursor.execute(ths_037_sql, i)
            ths_conn.commit()
        except Exception, e:
            print e
        ths_037 = ths_cursor.fetchall()

        insert_sql = "insert into people_basicinfo " + " (people_char, name, sex_par, birth_day, country, poli_status, university, high_edu, image, back_gro) " + " values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        name = ths_037[0][4]
        if name is None:
            name = ''

        if ths_037[0][5] == 'm':
            sex_par = 0
        elif ths_037[0][5] == 'f':
            sex_par = 1
        else:
            sex_par = 2
        birth_day = ths_037[0][6]

        if ths_037[0][9] is not None:
            try:
                country = ths_007_code[ths_007_num.index(ths_037[0][9])]
            except Exception, e:
                if u',' in ths_037[0][9]:
                    country = ths_007_code[ths_007_num.index(ths_037[0][9].split(u',')[0])]
                else:
                    country = '100000'
        else:
            if ths_037[0][10] is not None:
                if ths_037[0][10] in ths_007_name:
                    country = ths_007_code[ths_007_name.index(ths_037[0][10])]
                else:
                    country = '100000'
            else:
                country = '100000'

        high_edu = ths_037[0][8]
        if high_edu is None:
            high_edu = ''

        back_gro = ths_037[0][11]

        try:
            datacenter_cursor.execute(insert_sql, (0, name, sex_par, birth_day, country, '', '', high_edu, '', back_gro))
            new_num += 1
            datacenter_conn.commit()
        except Exception, e:
            print e

        print new_num, ths_037[0][0], name, country


