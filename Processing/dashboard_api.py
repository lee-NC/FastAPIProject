import io
import os
import sys
from datetime import datetime, timedelta
import traceback
import pandas as pd
import pyarrow as pa
import logging
import pyarrow.parquet as pq
from dateutil.relativedelta import relativedelta
from telegram import Bot
from telegram import InputFile
from Helper.config import Config, init_connect_report, init_connect_tele
from Model.DataModel import *

sys.path.append(os.path.abspath("HBase/Model"))
logger = logging.getLogger("Lakehouse")

config = Config()


def _get_total_income(start_date, end_date):
    (hbase_connection, hdfs_client) = init_connect_report(config)
    table = hbase_connection.table('PERSONAL_SIGN_TURN_ORDER')
    filter_start = f"SingleColumnValueFilter('info', 'req_time', >=, 'binary:{int(start_date.timestamp() * 1000)}')"
    filter_end = f"SingleColumnValueFilter('info', 'req_time', <, 'binary:{int(end_date.timestamp() * 1000)}')"
    filters = f"{filter_start} AND {filter_end}"
    rows = table.scan(filter=filters)
    total_money = 0
    for key, data in rows:
        total_money += int(data.get(b'INFO:TOTAL_MONEY'))
    return total_money
