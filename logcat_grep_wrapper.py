#!/usr/bin/python
# -*- coding: utf-8 -*-
import fire
from pyhive import hive
import pandas as pd
import arrow
import commands
import os

SEC_IN_ONE_HOUR = (24 * 3600)
DEFAULT_QUERY_UPC_DAYS = 30
VERSION_CODE = "8.0.0.506909"
LOG_DIR_LIST = ["/usr/local/seven/usa-ap01/logs/flume/",
            "/usr/local/seven/usa-ap02/logs/flume/"]

DF_UPC_CACHE_FILE_NAME = "df_upc.cache"


conn = hive.Connection(host="ap04.usa.7sys.net",
                       port=10000, username=None,
                       configuration={'hive.resultset.use.unique.column.names':'false'})


def cal_time_period(query_days=DEFAULT_QUERY_UPC_DAYS):
    utc_now = arrow.utcnow()
    data_entry_start = utc_now.shift(days=-query_days).timestamp / SEC_IN_ONE_HOUR * SEC_IN_ONE_HOUR * 1000
    date_entry_end = utc_now.shift(days=+1).timestamp / SEC_IN_ONE_HOUR * SEC_IN_ONE_HOUR * 1000
    return data_entry_start,date_entry_end


def query_user_first_upgrage_time(version=VERSION_CODE):
    if os.path.exists(DF_UPC_CACHE_FILE_NAME):
        try:
            mtime = os.path.getmtime(DF_UPC_CACHE_FILE_NAME)
            now = arrow.now().timestamp
            if (now-mtime) <= 24*3600:
                df = pd.read_pickle(DF_UPC_CACHE_FILE_NAME)
                return df
        except Exception,e:
            print e
    try:
        os.remove(DF_UPC_CACHE_FILE_NAME)
    except Exception, e:
        print e

    data_entry_start,date_entry_end = cal_time_period()
    sql_for_upc = '''
    select user_id,MIN(ts)
    from cr_upc 
    where version = '{}'
    AND date_entry >= {}
    AND date_entry <= {}
    group by user_id
    '''.format(version, data_entry_start, date_entry_end)

    print sql_for_upc
    df = pd.read_sql(sql_for_upc, conn)
    df.to_pickle(DF_UPC_CACHE_FILE_NAME)
    return df


def read_log_file_list():
    file_list = []
    for dir in LOG_DIR_LIST:
        if os.path.exists(dir):
            cmd_result = commands.getstatusoutput("ls {}/aggregated*".format(dir))
            if cmd_result[0] == 0:
                file_list = cmd_result[1].split("\n")

    return file_list

def read_log_file_list_with_upc_time():
    file_list_filter = []
    df = query_user_first_upgrage_time()
    min_time = df.iloc[:,1].min()/1000
    print min_time
    file_list = read_log_file_list()
    for file in file_list:
        mtime = os.path.getmtime(file)
        #print mtime
        if mtime <= min_time:
            continue
        print mtime
        file_list_filter.append(file)


    return file_list_filter


class MainWrapper(object):
    def run(self):
        df = query_user_first_upgrage_time()
        pass


    def test_read_log_file_list(self):
        file_list = read_log_file_list()
        print file_list
        pass

    def test_log_file_filter(self):
        file_list = read_log_file_list_with_upc_time()
        print file_list

    def test_upc_log_query(self):
        df = query_user_first_upgrage_time()
        print df
        pass

    pass


def main():
    fire.Fire(MainWrapper)
    pass


if __name__ == "__main__":
    main()
