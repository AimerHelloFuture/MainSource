#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from mysql import mysqlUri
from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
from mailhelper import MailHelper


LOG_FILENAME = "./log_everyday_com_balance.txt"


class GetInfo:
    def __init__(self):
        pass

    def get_report_type(self, F013_THS013, F014_THS013):

        report_type = 1015000
        if F013_THS013 is None or F014_THS013 is None:
            return report_type

        if u'定期报告' in F013_THS013:
            if u'一季度' in F014_THS013:
                report_type = 1015001
            elif u'半年度' in F014_THS013:
                report_type = 1015002
            elif u'三季度' in F014_THS013:
                report_type = 1015003
            elif u'年度' in F014_THS013:
                report_type = 1015004
        else:
            if u'招股说明书' == F013_THS013:
                report_type = 1015014
            elif u'债券招募说明书' == F013_THS013:
                report_type = 1015005
            elif u'三板转让说明书' == F013_THS013:
                report_type = 1015013
            elif u'招股说明书申报稿' == F013_THS013:
                report_type = 1015010

        return report_type

    def get_date(self):
        config_ths = uri.get_ths_mysql_uri()
        ths_list = uri.start_ths_MySQL(config_ths)
        ths_conn = ths_list[0]
        ths_cursor = ths_list[1]

        date_sql = 'select DISTINCT F003_THS013 from ths013 ORDER BY F003_THS013 desc'
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


class EverydayComBalance:
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

    def get_com_uni_code(self, F001_THS013):
        try:
            F002_THS002 = self.f002_ths002_list[self.f001_ths002_list.index(F001_THS013)]
        except Exception, e:
            F002_THS002 = None
        try:
            com_uni_code = self.center_code_list[self.center_name_list.index(F002_THS002)]
        except Exception, e:
            com_uni_code = None
        return com_uni_code

    def insert_data(self, data):
        com_uni_code = self.get_com_uni_code(F001_THS013=data[3])
        end_date = data[5]
        report_type = self.info.get_report_type(F013_THS013=data[15], F014_THS013=data[16])

        principles = 1502002 if data[11] == '595001' else 1502001

        if data[18] == 'CNY':
            currency_code = 1011001
        elif data[18] == 'USD':
            currency_code = 1011002
        elif data[18] == 'EUR':
            currency_code = 1011004
        elif data[18] == 'PKR':
            currency_code = 1011008
        else:
            currency_code = 1011009

        if data[6] == 'HB':
            consolidation = 1501002
        elif data[6] == 'MGS':
            consolidation = 1501004
        elif data[6] == 'HBTZ':
            consolidation = 1501001
        else:  # data[6] == 'MGSTZ':
            consolidation = 1501003

        announcement_date = data[20]

        if data[10] == u'一般企业':
            report_format = 1013025
        elif data[10] == u'商业银行':
            report_format = 1013002
        elif data[10] == u'保险公司':
            report_format = 1013003
        else:
            report_format = 1013006

        keys = 'com_uni_code,end_date,report_type,principles,currency_code,consolidation,announcement_date,report_format,cash,trading_fin_assets,rec_note,rec_account,prepay,rec_interest,rec_dividend,other_rec_account,inventory,non_current_asset,other_current_asset,current_assets_special_subjects,current_asset_of_balance,total_current_asset,available_sale_asset,held_investment,long_rec_account,long_equity_investment,invest_house,fix_asset,building,balance_account_asset,fix_asset_dispose,product_asset,oil_asset,intangible_asset,develop_cost,goodwill,long_defer_cost,tax_asset,other_noncurrent_asset,noncurrent_asset_special_subjects,noncurrent_asset_of_balance,total_noncurrent_asset,cash_depo_cenbank,depo_other_bank,expensive_mental,disassemble_fund,derivat_fin_asset,buy_fin_asset,loan_advance,other_asset,rec_loan_account,recei_premium,receivable_subrogation,recei_dividend_payment,recei_unearned_r,recei_claims_r,recei_life_r,recei_long_health_r,insurer_impawn_loan,fixed_time_deposit,save_capital_deposit,independ_account_assets,customer_funds_deposit,settlement_provisions,customer_payment,refundable_deposits,transaction_fee,asset_special_subject,asset_balance_subject,total_asset,short_borrow,transation_fin_borrow,pay_note,pay_account,prepay_account,pay_salary,pay_tax,pay_interest,pay_dividend,other_pay_account,non_current_borrow,other_current_borrow,current_lia_special_subjects,current_lia_balance_subjects,total_current_liabilities,long_borrow,pay_bonds,long_pay_account,term_pay_account,pre_bonds,deferred_income,other_noncurrent_bonds,non_current_lia_special_subjects,non_current_lia_balance_subjects,total_non_current_liabilities,borrow_central,peer_other_fin_depo_pay,borrow_fund,derivat_fin_liabilities,sell_fin_asset,absorb_depo,other_liabilities,advance_insurance,payable_fee,pay_account_rein,compensation_payable,policy_dividend_payable,insurer_deposit_investment,unearned_premium_reserve,outstanding_claim_reserve,life_insurance_reserve,long_health_insurance_lr,independ_liabilities,pledged_loan,agent_trading_secrity,act_underwrite_securities,liabilities_special_subjects,liabilities_balance_subjects,total_liabilities,rec_capital,capital_reserve,treasury_stock,special_reserve,earn_reserve,general_normal_preparation,general_risk_preparation,nopay_profit,translation_reserve,shareholders_equity_special_subject,shareholders_equity_balance_subject,total_account_parent_equity,monority_holder_equity,total_account_equity,total_account_equity_and_lia,whether_published,special_case_description,total_number_of_shares,recei_dividend_contract,absorb_depo_and_interbank,insur_contract_reserves,deferred_income_current_lia,short_term_bonds_payable,non_current_lia_deferred_income,deposits_received,financial_capital,receivables,short_term_financing,payables,other_comprehensive_income,long_term_pay_for_employees,other_equity_instruments,preferred_stock,permanent_debt,come_source'
        number = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s'
        values = [com_uni_code,end_date,report_type,principles,currency_code,consolidation,announcement_date,report_format]
        insert_sql = "insert into com_balance" + " ( " + keys + " ) " + " values (" + number + ")"
        for value_i in range(21, 166):
            values.append(data[value_i])
        values.append(u'同花顺-data')

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
        update_sql = "update com_balance set announcement_date=%s,cash=%s,trading_fin_assets=%s,rec_note=%s,rec_account=%s,prepay=%s,rec_interest=%s,rec_dividend=%s,other_rec_account=%s,inventory=%s,non_current_asset=%s,other_current_asset=%s,current_assets_special_subjects=%s,current_asset_of_balance=%s,total_current_asset=%s,available_sale_asset=%s,held_investment=%s,long_rec_account=%s,long_equity_investment=%s,invest_house=%s,fix_asset=%s,building=%s,balance_account_asset=%s,fix_asset_dispose=%s,product_asset=%s,oil_asset=%s,intangible_asset=%s,develop_cost=%s,goodwill=%s,long_defer_cost=%s,tax_asset=%s,other_noncurrent_asset=%s,noncurrent_asset_special_subjects=%s,noncurrent_asset_of_balance=%s,total_noncurrent_asset=%s,cash_depo_cenbank=%s,depo_other_bank=%s,expensive_mental=%s,disassemble_fund=%s,derivat_fin_asset=%s,buy_fin_asset=%s,loan_advance=%s,other_asset=%s,rec_loan_account=%s,recei_premium=%s,receivable_subrogation=%s,recei_dividend_payment=%s,recei_unearned_r=%s,recei_claims_r=%s,recei_life_r=%s,recei_long_health_r=%s,insurer_impawn_loan=%s,fixed_time_deposit=%s,save_capital_deposit=%s,independ_account_assets=%s,customer_funds_deposit=%s,settlement_provisions=%s,customer_payment=%s,refundable_deposits=%s,transaction_fee=%s,asset_special_subject=%s,asset_balance_subject=%s,total_asset=%s,short_borrow=%s,transation_fin_borrow=%s,pay_note=%s,pay_account=%s,prepay_account=%s,pay_salary=%s,pay_tax=%s,pay_interest=%s,pay_dividend=%s,other_pay_account=%s,non_current_borrow=%s,other_current_borrow=%s,current_lia_special_subjects=%s,current_lia_balance_subjects=%s,total_current_liabilities=%s,long_borrow=%s,pay_bonds=%s,long_pay_account=%s,term_pay_account=%s,pre_bonds=%s,deferred_income=%s,other_noncurrent_bonds=%s,non_current_lia_special_subjects=%s,non_current_lia_balance_subjects=%s,total_non_current_liabilities=%s,borrow_central=%s,peer_other_fin_depo_pay=%s,borrow_fund=%s,derivat_fin_liabilities=%s,sell_fin_asset=%s,absorb_depo=%s,other_liabilities=%s,advance_insurance=%s,payable_fee=%s,pay_account_rein=%s,compensation_payable=%s,policy_dividend_payable=%s,insurer_deposit_investment=%s,unearned_premium_reserve=%s,outstanding_claim_reserve=%s,life_insurance_reserve=%s,long_health_insurance_lr=%s,independ_liabilities=%s,pledged_loan=%s,agent_trading_secrity=%s,act_underwrite_securities=%s,liabilities_special_subjects=%s,liabilities_balance_subjects=%s,total_liabilities=%s,rec_capital=%s,capital_reserve=%s,treasury_stock=%s,special_reserve=%s,earn_reserve=%s,general_normal_preparation=%s,general_risk_preparation=%s,nopay_profit=%s,translation_reserve=%s,shareholders_equity_special_subject=%s,shareholders_equity_balance_subject=%s,total_account_parent_equity=%s,monority_holder_equity=%s,total_account_equity=%s,total_account_equity_and_lia=%s,whether_published=%s,special_case_description=%s,total_number_of_shares=%s,recei_dividend_contract=%s,absorb_depo_and_interbank=%s,insur_contract_reserves=%s,deferred_income_current_lia=%s,short_term_bonds_payable=%s,non_current_lia_deferred_income=%s,deposits_received=%s,financial_capital=%s,receivables=%s,short_term_financing=%s,payables=%s,other_comprehensive_income=%s,long_term_pay_for_employees=%s,other_equity_instruments=%s,preferred_stock=%s,permanent_debt=%s where id =%s"
        center_id = center_com_balance[0][0]
        center_announcement = center_com_balance[0][7]
        ths_announcement = new_data[20]
        # center_rtime = center_com_balance[0][7]
        # ths_rtime = new_data[2]
        if center_announcement is None:  # 中心库公告日期为空

            if ths_announcement is not None:  # 同花顺公告日期不为空才更新若为空则不变

                values = [ths_announcement]
                for value_i in range(21, 166):
                    values.append(new_data[value_i])
                values.append(center_id)

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

                values = [ths_announcement]
                for value_i in range(21, 166):
                    values.append(new_data[value_i])
                values.append(center_id)

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

        ths_013_sql3 = 'SELECT * from ths013 WHERE RTIME_THS013 > %s'
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
        end_date = new_data[5]
        if new_data[6] == 'HB':
            consolidation = 1501002
        elif new_data[6] == 'MGS':
            consolidation = 1501004
        elif new_data[6] == 'HBTZ':
            consolidation = 1501001
        else:  # data[6] == 'MGSTZ':
            consolidation = 1501003

        come_source = u'同花顺-data'

        center_balance_sql0 = 'SELECT * from com_balance WHERE com_uni_code = %s and end_date = %s and consolidation = %s and come_source = %s'

        config_datacenter = uri.get_datacenter_mysql_uri()
        datacenter_list = uri.start_datacenter_MySQL(config_datacenter)
        datacenter_conn = datacenter_list[0]
        datacenter_cursor = datacenter_list[1]

        try:
            datacenter_cursor.execute(center_balance_sql0, (com_uni_code, end_date, consolidation, come_source))
        except Exception, e:
            logging.error(e)
        center_com_balance = datacenter_cursor.fetchall()
        uri.close_datacenter_MySQL(datacenter_conn, datacenter_cursor)
        return center_com_balance

    def work(self):

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
        today = today + 'everyday_com_balance'
        mail_helper = MailHelper()
        try:
            mail_helper.send_mail_to(dest='ycckevinyang@163.com', message=today)
        except Exception, e:
            logging.error(e)
        everyday_com_balance = EverydayComBalance()
        everyday_com_balance.work()


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S', filename=LOG_FILENAME, filemode='w')

    uri = mysqlUri()
    spider = Spider()

    scheduler = BlockingScheduler()
    # scheduler.add_job(spider.work, 'date', run_date=datetime.datetime.now())
    scheduler.add_job(spider.work, 'cron', day_of_week='0-6', hour='9,18', minute=13, end_date='2018-08-01')

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()



