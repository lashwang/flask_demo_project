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


LOG_DIR_LIST = ["/usr/local/seven/usa-ap01/logs/flume/",
            "/usr/local/seven/usa-ap02/logs/flume/"]


conn = hive.Connection(host="ap04.usa.7sys.net",
                       port=10000, username=None,
                       configuration={'hive.resultset.use.unique.column.names':'false'})


def cal_time_period(query_days=DEFAULT_QUERY_UPC_DAYS):
    utc_now = arrow.utcnow()
    data_entry_start = utc_now.shift(days=-query_days).timestamp / SEC_IN_ONE_HOUR * SEC_IN_ONE_HOUR * 1000
    date_entry_end = utc_now.shift(days=+1).timestamp / SEC_IN_ONE_HOUR * SEC_IN_ONE_HOUR * 1000
    return data_entry_start,date_entry_end


def query_user_first_upgrage_time(version):
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

    return df


def read_log_file_list():
    for dir in LOG_DIR_LIST:
        if os.path.exists(dir):
            cmd_result = commands.getstatusoutput("ls {}/aggregated*".format(dir))
            print cmd_result[0]


class MainWrapper(object):
    def run(self):
        df = query_user_first_upgrage_time(version="8.0.0.506909")
        pass


    def test_module(self):
        read_log_file_list()
        pass

    pass


def main():
    fire.Fire(MainWrapper)
    pass


if __name__ == "__main__":
    main()
