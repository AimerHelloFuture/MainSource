#!/usr/bin/env python
# -*- coding: utf-8 -*-
import xlrd
import xlwt

from mysql_cp import mysqlUri

data = xlrd.open_workbook('lose.xlsx')
sheet = data.sheet_by_index(0)

workbook = xlwt.Workbook(encoding='utf-8')
data_sheet = workbook.add_sheet(u'表')

uri = mysqlUri()

config_ths = uri.get_ths_mysql_uri()
ths_list = uri.start_ths_MySQL(config_ths)
ths_conn = ths_list[0]
ths_cursor = ths_list[1]

for r in range(0, sheet.nrows):
    sec_code = str(int(sheet.cell(r, 0).value))
    sec_code = sec_code.zfill(6)

    select_sql2 = 'SELECT F012_THS001 from ths001 WHERE F002_THS001 = %s'

    try:
        ths_cursor.execute(select_sql2, (sec_code,))
    except Exception, e:
        print e
    result2 = ths_cursor.fetchall()

    f001_ths002 = result2[0][0]
    print r, f001_ths002

    data_sheet.write(r, 0, f001_ths002)

workbook.save('check.xlsx')

uri.close_ths_MySQL(ths_conn, ths_cursor)










