import pymysql
import paramiko
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from os.path import expanduser
import datetime
from datetime import timedelta
# SSH Tunnel and connection setup- Main-DB-Read Main DB 的連線參數
mypkey = paramiko.RSAKey.from_private_key_file("D:\\aws\\us-west-spider.pem")
sql_hostname = "127.0.0.1"
sql_username = "read_only"
sql_password = "eland1234"  # 此文件不將機敏密碼輸入，但這個資料庫的密碼使用的是內部慣例的密碼
sql_main_database = 'dmp_stat'
sql_port = 3306
ssh_host = "54.212.237.220"
ssh_user = "root"
ssh_port = 22


# 先 define 一個方便輸入日期範圍的功能，供後續主要功能- log_extractor 使用
def DateFormatter(date_string):
    '''
    :param date_string: input date in format as 20180510
    :return: datetime library readied date format
    '''
    year = str(date_string)[:4]
    month = str(date_string)[4:6]
    day = str(date_string)[-2:]
    if month[0] == 0:
        month = str(date_string)[5:6]
    if day[0] == 0:
        day = str(date_string)[-1:]
    result = datetime.date(int(year), int(month), int(day))
    return result


# 真正撈資料的功能 input: 時間範圍、需要欄位, url_pattern也就是要 url like 的 pattern；output: 指定的 log 存成 pandas dataframe
def log_extractor(date_range, columns_needed, **kwargs):
    '''
    :param date_range: input the date range in format  of [yyyymmdd, yyyymmdd] ex: [20190310,20190311]
    :param columns_needed: input all the columns needed as column1, column2, ...
    還是得記得去改下面 query 以符合自己的需要唷(where 後面的語句)，這邊支援 datetime的使用，會用SQL的方式自己把
    需要的日期串上去(因為 dmp_stat 裡面是沒有日期的) ex: ['datetime', 'url', 'uid']
    :param url_pattern: 如果有要使用 url_pattern 撈 log, 則需指定 url_pattern = "www.kimy.com.tw"`,
    實際上執行 query 時則會以 url like "%www.kimy.com.tw%" 進行
    :return: dataframe with the log file input
    '''
    print("The mission starts at... " + str(datetime.datetime.now()))
    url_pattern = kwargs.get('url_pattern', '')
    d1 = DateFormatter(date_range[0])
    d2 = DateFormatter(date_range[1])
    delta = d2 - d1
    result_dataframe = pd.DataFrame()
    columns = str()
    if 'datetime' in columns_needed:
        columns_needed.remove('datetime')
        for j in columns_needed:
            columns += '\'' + j + '\','
        columns = columns[:-1]
        columns = columns.replace("\'", "`")
        for i in range(delta.days + 1):
            date = d1 + timedelta(i)
            with SSHTunnelForwarder(
                    (ssh_host, ssh_port),
                    ssh_username=ssh_user,
                    ssh_pkey=mypkey,
                    remote_bind_address=(sql_hostname, sql_port)) as tunnel:
                conn = pymysql.connect(host='127.0.0.1', user=sql_username,
                                       passwd=sql_password, db=sql_main_database,
                                       port=tunnel.local_bind_port)
                query = "select STR_TO_DATE(CONCAT(%s,' ',`hour`,':',`minute`,':',`second`), '%%Y%%m%%d %%H:%%i:%%s') AS `datetime`, %s from dmp_stat.%s where `url` like \"%%%s%%\"" % (
                str(date).replace("-", ""), columns, str(date).replace("-", ""), str(url_pattern))
                data = pd.read_sql_query(query, conn)
                result_dataframe = result_dataframe.append(data, ignore_index=True)
                conn.close()
        print("The mission ends at... " + str(datetime.datetime.now()))
        return result_dataframe
    else:
        columns = str()
        for j in columns_needed:
            columns += '\'' + j + '\','
        columns = columns[:-1]
        columns = columns.replace("\'", "`")
        for i in range(delta.days + 1):
            date = d1 + timedelta(i)
            with SSHTunnelForwarder(
                    (ssh_host, ssh_port),
                    ssh_username=ssh_user,
                    ssh_pkey=mypkey,
                    remote_bind_address=(sql_hostname, sql_port)) as tunnel:
                conn = pymysql.connect(host='127.0.0.1', user=sql_username,
                                       passwd=sql_password, db=sql_main_database,
                                       port=tunnel.local_bind_port)

                query = '''select %s from dmp_stat.%s where `url` like \"%%%s%%\"''' % (
                columns, str(date).replace("-", ""), str(url_pattern))
                data = pd.read_sql_query(query, conn)
                result_dataframe = result_dataframe.append(data, ignore_index=True)
                conn.close()
        print("The mission ends at... " + str(datetime.datetime.now()))
        return result_dataframe