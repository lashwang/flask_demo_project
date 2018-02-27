#!/usr/bin/python
# -*- coding: utf-8 -*-
import fire
from pyhive import hive
import pandas as pd
import arrow
import commands
import os
from io import BytesIO
import zlib

SEC_IN_ONE_HOUR = (24 * 3600)
DEFAULT_QUERY_UPC_DAYS = 30
DEFAULT_VERSION_CODE = "8.0.0.506909"
LOG_DIR_LIST = ["/usr/local/seven/usa-ap01/logs/flume/",
            "/usr/local/seven/usa-ap02/logs/flume/"]

DF_UPC_CACHE_FILE_NAME = "df_upc.cache"
LOGS_TYPE_CRCS = 3
LOGS_TYPE_CRCS_EXTRA = 5
LOGS_TYPE_GUDS = 91
LOGS_TYPE_TERRA_UPC = 92
LOGS_TYPE_OTHERS = 99
LOGS_TYPE_INFO_ONLY = 100 # Only get aggregated file index

LOGS_TYPES = {0:{'name':'logcat',
                 'suffix':'.log'},
              1:{'name':'tcpdump',
                 'suffix':'.pcap'},
              2:{'name':'iptables',
                 'suffix':'.log'},
              3:{'name':'crcs',
                 'suffix':'.avro'},
              4:{'name':'qoe',
                 'suffix':'.log'},
              5:{'name':'crcs',
                 'suffix':'.avro'},
              LOGS_TYPE_GUDS:{'name':'guds',
                  'suffix':'.log'},
              LOGS_TYPE_TERRA_UPC:{'name':'crcs',
                 'suffix':'.avro'},
              LOGS_TYPE_OTHERS:{'name':'others',
                  'suffix':'.log'},
              LOGS_TYPE_INFO_ONLY:{'name':'info',
                  'suffix':'.info'}
            }

conn = hive.Connection(host="ap04.usa.7sys.net",
                       port=10000, username=None,
                       configuration={'hive.resultset.use.unique.column.names':'false'})


global_upc_df = pd.DataFrame()

def cal_time_period(query_days=DEFAULT_QUERY_UPC_DAYS):
    utc_now = arrow.utcnow()
    data_entry_start = utc_now.shift(days=-query_days).timestamp / SEC_IN_ONE_HOUR * SEC_IN_ONE_HOUR * 1000
    date_entry_end = utc_now.shift(days=+1).timestamp / SEC_IN_ONE_HOUR * SEC_IN_ONE_HOUR * 1000
    return data_entry_start,date_entry_end


def query_user_first_upgrage_time(version=DEFAULT_VERSION_CODE):
    global global_upc_df

    if os.path.exists(DF_UPC_CACHE_FILE_NAME):
        try:
            mtime = os.path.getmtime(DF_UPC_CACHE_FILE_NAME)
            now = arrow.now().timestamp
            if (now-mtime) <= 24*3600:
                global_upc_df = pd.read_pickle(DF_UPC_CACHE_FILE_NAME)
                return
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
    global_upc_df = pd.read_sql(sql_for_upc, conn)
    global_upc_df.to_pickle(DF_UPC_CACHE_FILE_NAME)


def read_log_file_list():
    file_list = []
    for dir in LOG_DIR_LIST:
        if os.path.exists(dir):
            cmd_result = commands.getstatusoutput("ls {}/aggregated*".format(dir))
            if cmd_result[0] == 0:
                file_list = cmd_result[1].split("\n")

    return file_list

def read_log_file_list_with_upc_time(upc_df):
    file_list_filter = []

    min_time = upc_df.iloc[:,1].min()/1000
    print min_time
    file_list = read_log_file_list()
    for file in file_list:
        # Get the last modify time.
        mtime = os.path.getmtime(file)
        if mtime <= min_time:
            continue
        file_list_filter.append(file)


    return file_list_filter

def toInt(bytesArr, pos, length):
    if len(bytesArr) < (pos + length):
        # sys.exit(-1)
        raise ValueError

    ret = 0
    offset = 0
    while True:
        if offset == 4 or offset == length: break
        ret = ret << 8 | ord(bytesArr[pos + offset])
        offset = offset + 1

    return ret

def toClientAddr(bytesArr, pos):
    nocIdInstanceId = toInt(bytesArr, pos, 4)
    nocId = nocIdInstanceId >> 8
    instanceId = nocIdInstanceId & 0x00ff
    hostId = toInt(bytesArr, pos + 4, 4)

    nocStr = str(hex(nocId)).replace('0x', "")
    hostStr = str(hex(hostId)).replace('0x', "")
    instanceStr = str(hex(instanceId)).replace('0x', "")
    return nocStr + "-" + hostStr + "-" + instanceStr

def toClientAddrHash(bytesArr):
    pckuserId = bytesArr.decode("utf-8")
    i = pckuserId.find(b"\x00")
    if i <= 0:
        return pckuserId
    else:
        return pckuserId[0:i]


def normalize_userid(user_id):
    if "||" in user_id:
        user_id = user_id.split("||")[0]

    return user_id


def read_block_head(blockhead):
    next_pos = 0
    pck_log_type = toInt(blockhead, next_pos, 1)
    next_pos += 1
    pck_log_level = toInt(blockhead, next_pos, 2)
    next_pos += 2
    pck_start_time = toInt(blockhead, next_pos, 4)
    next_pos += 4
    pck_end_time = toInt(blockhead, next_pos, 4)
    next_pos += 4
    pckPayloadSize = toInt(blockhead, next_pos, 4)
    next_pos += 4

    return pck_log_type,pck_log_level,pck_start_time,pck_end_time,pckPayloadSize

def read_aggregated_file(aggregated_log_file,on_user_filter=None,on_logcat_filter=None):
    binaryFile = open(aggregated_log_file, 'rb')

    try:
        total_size = os.path.getsize(aggregated_log_file)
        next_position = 0
        block_index = 0
        while True:
            blockhead = binaryFile.read(5)  # read block head
            if not blockhead:
                print 'not header'
                break
            pck_size = toInt(blockhead, 0, 4)
            ver = toInt(blockhead, 4, 1)
            if ver > 2:
                break
            if ver == 1:
                addrs_data = binaryFile.read(8)
                pckuserId = toClientAddr(addrs_data, 0)
            elif ver == 2:
                user_id_bytes = binaryFile.read(128)
                pckuserId = toClientAddrHash(user_id_bytes)

            blockhead = binaryFile.read(15)
            pck_log_type, \
            pck_log_level, \
            pck_start_time, \
            pck_end_time, \
            pckPayloadSize = read_block_head(blockhead)
            next_position += pck_size
            if not (pckPayloadSize > 0):
                print ("the log with invliad ver found!  version:%d ===> exit" % (ver))
                break
            if next_position > total_size:
                print (
                        "Block [%d] payload_data is not complete, next_position %d > total_size %d. aggregated_log_file is %s" \
                        % (block_index, next_position, total_size, aggregated_log_file))
                break
            block_index += 1
            log_tpype_info = LOGS_TYPES.get(pck_log_type)
            if pck_log_type != 0:
                # print ("pck_log_type %d is NOT supported" % pck_log_type)
                binaryFile.seek(pckPayloadSize, 1)
                continue

            pckuserId = normalize_userid(pckuserId)
            #print "read user {} block".format(pckuserId)
            if not on_user_filter or on_user_filter(pckuserId,pck_start_time,pck_end_time):
                read_block(binaryFile, pckPayloadSize,pckuserId,on_logcat_filter)
        # end while
    except Exception,e:
        print e

    finally:
        binaryFile.close()
    # end try:


def read_block(binaryFile, pckPayloadSize,pckuserId,on_logcat_filter=None):
    bytesNeedsToWrite = pckPayloadSize
    payload = BytesIO()
    try:
        while bytesNeedsToWrite > 0:
            curLen = 5120
            if bytesNeedsToWrite < 5120: curLen = bytesNeedsToWrite
            blockBody = binaryFile.read(curLen)  # 5M  per writing
            payload.write(blockBody)
            bytesNeedsToWrite = bytesNeedsToWrite - curLen
        # end while

        try:
            payload_data = zlib.decompress(payload.getvalue(), zlib.MAX_WBITS | 16)
            print "get logcat content size: {}".format(len(payload_data))
            if on_logcat_filter:
                on_logcat_filter(pckuserId,payload_data)
        except Exception, error:
            print error

    finally:
        payload.close()


def global_on_user_filter(pckuserId,pck_start_time,pck_end_time):
    global global_upc_df
    df_user = global_upc_df[global_upc_df.user_id == pckuserId]
    if df_user.empty:
        return False
    print df_user
    start_time = df_user._c1

    print start_time


    return True
    pass

def global_on_logcat_filter(pckuserId,payload_data):
    pass


class MainWrapper(object):
    def run(self):
        global global_upc_df

        query_user_first_upgrage_time()
        file_list = read_log_file_list_with_upc_time(global_upc_df)
        for file in file_list:
            print file
            read_aggregated_file(file,global_on_user_filter,global_on_logcat_filter)
        pass


    def test_read_log_file_list(self):
        file_list = read_log_file_list()
        print file_list
        pass

    def test_log_file_filter(self):
        global global_upc_df
        query_user_first_upgrage_time()
        file_list = read_log_file_list_with_upc_time(global_upc_df)
        print file_list

    def test_upc_log_query(self):
        global global_upc_df
        query_user_first_upgrage_time()
        print global_upc_df
        pass

    def test_agg_file_parser(self):
        global global_upc_df
        query_user_first_upgrage_time()
        read_aggregated_file('test_data/aggregated0',global_on_user_filter,global_on_logcat_filter)

    pass


def main():
    fire.Fire(MainWrapper)
    pass


if __name__ == "__main__":
    main()
