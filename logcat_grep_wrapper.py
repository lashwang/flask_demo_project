#!/usr/bin/python
# -*- coding: utf-8 -*-
import fire
from pyhive import hive
import pandas as pd
import arrow

conn = hive.Connection(host="ap04.usa.7sys.net",
                       port=10000, username=None,
                       configuration={'hive.resultset.use.unique.column.names':'false'})



def query_user_first_upgrage_time(version):
    utc_now = arrow.utcnow()
    data_entry_start = utc_now.shift(days=-30).timestamp / (24 * 3600) * (24 * 3600) * 1000
    date_entry_end = utc_now.shift(days=+1).timestamp / (24 * 3600) * (24 * 3600) * 1000

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


def main():
    df = query_user_first_upgrage_time(version = "8.0.0.506909")
    pass


if __name__ == "__main__":
    main()
