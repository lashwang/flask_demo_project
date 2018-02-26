#!/usr/bin/python
# -*- coding: utf-8 -*-
import fire
from pyhive import hive
import pandas as pd
import arrow


SEC_IN_ONE_HOUR = (24 * 3600)

DEFAULT_QUERY_UPC_DAYS = 30


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


class MainWrapper(object):
    def run(self):
        df = query_user_first_upgrage_time(version="8.0.0.506909")
        pass


    pass


def main():
    fire.Fire(MainWrapper)
    pass


if __name__ == "__main__":
    main()
