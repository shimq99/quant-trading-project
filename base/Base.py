# encoding:UTF-8
import os
import datetime
import time
import pyodbc
import logging
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from email.mime.image import MIMEImage   
from email.mime.multipart import MIMEMultipart
from tools import PandasDBUtils as pdUtil
import configparser
import pandas as pd

class Base(object):

    def __init__(self):
        pass

    def initSqlServer(self, env):
        (_database, _host, _user, _pwd) = self.getSQLCONFIG('config\db.config',env)
        self.conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+_host+';DATABASE='+_database+';UID='+_user+';PWD='+ _pwd, autocommit=True)
        self.cursor = self.conn.cursor() 
        self.cursor.fast_executemany = True       

    def closeSqlServerConnection(self):
        self.conn.commit()
        self.conn.close()

    def insertToDatabase(self, sql, data):
        if data:
            self.cursor.executemany(sql, data)
            # for record in data:
            #     try:
            #         self.cursor.execute(sql, (record))
            #     except Exception, e:
            #         logging.error(record)
            #         logging.error('data:' + e.args[1])
            #         raise Exception('error')
        else:
            logging.error('insertToDatabase: data is empty')
            raise Exception('insertToDatabase: data is empty')

    def upsert_result(self, data, db_table, db_index_cols =[], db_value_cols=[], dataframe_cols=[], updateIfExist=False, insert_without_check=False):
        records = pdUtil.dataFrameToSavableRecords(data, dataframe_cols)
        if insert_without_check:
            logging.info('insert without check  mode')
            insert_sql = 'insert into '+db_table+'(' + (',').join(db_index_cols+db_value_cols) + ') values(' + ('?,' * (len(db_index_cols+db_value_cols)))[:-1] + ')'
            self.insertToDatabase(insert_sql, records)
        else:
            logging.info('upsert mode')
            for d in records:
                try:
                    sql = 'select * from ' + db_table + ' where '+ ('=? and ').join(db_index_cols) + '=?'
                    self.cursor.execute(sql, tuple(d[:len(db_index_cols)]))
                    columns = [column[0] for column in self.cursor.description]
                    allResult = self.cursor.fetchall()
                    check_existed = pd.DataFrame((tuple(row) for row in allResult), columns=columns)
                    if check_existed.empty:
                        insert_sql = 'insert into '+db_table+'(' + (',').join(db_index_cols+db_value_cols) + ') values(' + ('?,' * (len(db_index_cols+db_value_cols)))[:-1] + ')'
                        self.insertToDatabase(insert_sql, [d])
                    elif updateIfExist:
                        update_sql = 'update '+db_table+' set '+('=?,').join(db_value_cols)+'=? where '+ ('=? and ').join(db_index_cols) + '=?'
                        self.cursor.execute(update_sql, tuple(d[len(db_index_cols):]+d[:len(db_index_cols)]))
                except Exception as e:
                    logging.error(d)
                    logging.error('upsert_result: error when saving '+e.message)
                    raise Exception('upsert_result: error when saving '+e.message)

    def sendMessageToMail(self, receivemailLists, subjectMsg, contentMsg, mailUserGroup = '', fileName = ''):
        mailAddress = 'itadmin@pinpointfund.com'
        mailPasswd = '5678#edc'
        self.sendMessageToMailGiveSender(mailAddress, mailPasswd, receivemailLists, subjectMsg, contentMsg, mailUserGroup, fileName)

    def sendMessageToMailGiveSender(self, mailAddress, mailPasswd, receivemailLists, subjectMsg, contentMsg, mailUserGroup = '', fileName = ''):
        smtp = smtplib.SMTP('smtp.office365.com', 587, timeout=60)
        smtp.starttls()
        smtp.ehlo()
        smtp.login(mailAddress, mailPasswd)
        msg = MIMEMultipart('related')
        receiverStr = ''
        for r in receivemailLists:
            receiverStr += r
            if (r != receivemailLists[-1]):
                receiverStr += ';'
        ##msg['To'] = ("%s") % (Header(receiverStr, 'utf-8'),)
        msg['To'] = receiverStr
        msg['From'] = ("%s<" + mailAddress + ">") % (Header(mailUserGroup, 'utf-8'),)
        msg['Subject'] = Header(subjectMsg, 'utf-8')
        message = MIMEText(contentMsg, 'html', 'gb2312')
        msg.attach(message)
        if (fileName != ''):
            att = MIMEText(open(fileName, 'rb').read(), 'base64', 'gb2312')
            att["Content-Type"] = 'application/octet-stream'
            filePaths = fileName.split('\\')
            extractFileName = filePaths[len(filePaths) - 1]
            fieldStr = 'attachment; filename=%s' % extractFileName.encode('gb2312')
            att["Content-Disposition"] = fieldStr
            msg.attach(att)
        smtp.sendmail(mailAddress, receivemailLists, msg.as_string())
        smtp.quit()

    def getSQLCONFIG(self, filename, env):
        work_dir = os.path.dirname(os.path.abspath(__file__))
        CONF_FILE = os.path.join(work_dir, filename)
        cf = configparser.ConfigParser()
        cf.read(CONF_FILE)
        _database = cf.get(env, "database")
        _host = cf.get(env, "host")
        _user = cf.get(env, "user")
        _pwd = cf.get(env, "pwd")
        return _database, _host, _user, _pwd

    def confirm_choice(self):
        confirm = input("[c]Confirm or [v]Void: ")
        if confirm != 'c' and confirm != 'v':
            print("\n Invalid Option. Please Enter a Valid Option.")
            return self.confirm_choice()
        print (confirm)
        return confirm

    def getOpenFigiCONFIG(self, filename,sectionName):
        work_dir = os.path.dirname(os.path.abspath(__file__))
        CONF_FILE = os.path.join(work_dir, filename)
        cf = configparser.ConfigParser()
        cf.read(CONF_FILE)
        CH_EQUITY_URL = cf.get(sectionName, 'CH_EQUITY_URL')
        HK_EQUITY_URL = cf.get(sectionName, "HK_EQUITY_URL")
        TT_EQUITY_URL = cf.get(sectionName, "TT_EQUITY_URL")
        US_ADR_URL = cf.get(sectionName, "US_ADR_URL")
        JP_EQUITY_URL = cf.get(sectionName, "JP_EQUITY_URL")
        KS_EQUITY_URL = cf.get(sectionName, "KS_EQUITY_URL")
        urlDict = dict()

        urlDict['China'] = CH_EQUITY_URL
        urlDict['HongKong'] = HK_EQUITY_URL
        urlDict['Taiwan'] = TT_EQUITY_URL
        urlDict['US'] = US_ADR_URL
        urlDict['Japan'] = JP_EQUITY_URL
        urlDict['Korea'] = KS_EQUITY_URL
        return urlDict