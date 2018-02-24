#!/usr/bin/python
# -*- coding: utf-8 -*-
import fire
from pyhive import hive
import pandas as pd


conn = hive.Connection(host="ap04.usa.7sys.net",
                       port=10000, username=None,
                       configuration={'hive.resultset.use.unique.column.names':'false'})



def main():
    sql_for_upc = '''
    select * from cr_upc 
where user_id = "deacf0c8-9365-4458-8007-2e72b1a4d32e"
AND date_entry >= 1517443200000
AND date_entry <= 1520640000000
'''

    df = pd.read_sql(sql_for_upc, conn)
    print df
    pass


if __name__ == "__main__":
    main()
