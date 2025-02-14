import io
import logging
import traceback
from datetime import datetime, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dateutil.relativedelta import relativedelta
from telegram import Bot
from telegram import InputFile

from helper.get_config import init_hdfs_connection, init_hbase_connection, init_connect_tele
from model.data_model import *

logger = logging.getLogger("Lakehouse")


async def processing_signature_transaction(date_now):
    hbase_connection = init_hbase_connection()
    temp_time = f"{date_now.year}-{date_now.month}-01T17:00:00.000Z"
    end_date = datetime.strptime(temp_time, "%Y-%m-%dT%H:%M:%S.%fZ") - timedelta(days=1)
    start_date = end_date - relativedelta(months=1)

    table = hbase_connection.table('SIGNATURE_TRANSACTION')
    filter_start = f"SingleColumnValueFilter('INFO', 'REQ_TIME', >=, 'binary:{int(start_date.timestamp() * 1000)}')"
    filter_end = f"SingleColumnValueFilter('INFO', 'REQ_TIME', <, 'binary:{int(end_date.timestamp() * 1000)}')"
    filters = f"{filter_start} AND {filter_end}"
    rows = table.scan(filter=filters)

    status_success = "1"
    parquet_file_name = f"warehouse/signature_by_client/signature_by_client_{end_date.year}.parquet"
    file_name = f"LuotKyTheoAppKy_{end_date.year}{end_date.month:02}{end_date.day:02}"
    # Lấy dữ liệu từ HBase
    allData = [['client_id', 'client_name', 'month', 'success', 'un_success']]
    # region processing
    try:
        for key, data in rows:
            appId = str(data.get(b'INFO:APP_ID').decode('utf-8'))
            appName = str(data.get(b'INFO:APP_NAME').decode('utf-8'))
            reqTime = float(str(data.get(b'INFO:REQ_TIME').decode('utf-8'))) / 1000
            month = datetime.fromtimestamp(timestamp=reqTime).strftime('%Y-%m')
            success = 0
            unSuccess = 0
            if data.get(b'INFO:STATUS').decode('utf-8') == status_success:
                success = 1
            else:
                unSuccess = 1
            allData.append([appId, appName, month, success, unSuccess])
    except Exception as e:
        print(f"Lỗi trong quá trình xử lý bản ghi: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    # endregion
    if len(allData) < 2:
        return "Không có bản ghi phù hợp"
    # Chuyển đổi dữ liệu thành DataFrame
    df = pd.DataFrame(allData[1:], columns=allData[0])

    # Nhóm dữ liệu theo ClientId và tính tổng số lượt ký
    grouped = df.groupby(['client_id', 'month']).agg({
        'client_name': 'first',
        'success': 'sum',
        'un_success': 'sum'
    }).reset_index()

    # region parquet
    table_parquet = grouped
    table_parquet['data_report'] = date_now.strftime("%Y/%m/%d %H:%M:%S")
    table_parquet = table_parquet[['data_report', 'client_id', 'client_name', 'month', 'success', 'un_success']]
    # Tạo schema cho file Parquet
    schema = pa.schema([
        ('data_report', pa.string()),
        ('client_id', pa.string()),
        ('client_name', pa.string()),
        ('month', pa.string()),
        ('success', pa.int32()),
        ('un_success', pa.int32())
    ])

    table_result = pa.Table.from_pandas(table_parquet, schema=schema)
    hdfs_client = init_hdfs_connection()
    mess = _save_file_hdfs(hdfs_client, parquet_file_name, table_result)
    if mess != "":
        return mess
    # endregion
    # region gửi file lên bot tele
    await send_excel_to_telegram(excel_buffer=_convert_to_excel(grouped), file_name=file_name)
    # endregion
    hbase_connection.close()
    return ""


async def job_signature_transaction():
    date_now = datetime.now()
    await processing_signature_transaction(date_now)


async def processing_accumulate_credential(date_now):
    hbase_connection = init_hbase_connection()
    temp_time = f"{date_now.year}-{date_now.month}-{date_now.day - 1}T17:00:00"
    end_date = datetime.strptime(temp_time, "%Y-%m-%dT%H:%M:%S")
    tm_end_date = end_date.timestamp()
    start_date = end_date - timedelta(days=1)
    tm_start_date = start_date.timestamp()
    start_month = datetime.strptime(f"{date_now.year}-{date_now.month}-01T17:00:00", "%Y-%m-%dT%H:%M:%S")
    start_month = start_month - timedelta(days=1)
    tm_start_month = start_month.timestamp()
    end_month = start_month + relativedelta(months=1)
    tm_end_month = end_month.timestamp()
    start_year = datetime.strptime(f"{date_now.year - 1}-12-31T17:00:00", "%Y-%m-%dT%H:%M:%S")
    tm_start_year = start_year.timestamp()
    end_year = start_year + relativedelta(years=1)
    tm_end_year = end_year.timestamp()
    file_name = f"bao_cao_san_luong_luy_ke_{end_date.year}{end_date.month:02}{end_date.day:02}"
    parquet_file_name = f"/warehouse/accumulate_credential/accumulate_credential_{end_date.year}.parquet"
    pricing_codes_ps0 = ["17187", "18046"]

    # dữ liệu bảng
    allData = []
    credential_ps = []

    # region Pm đang hoạt động và cts hết hạn
    table_credential = hbase_connection.table('CREDENTIAL')
    rows_credential = table_credential.scan()
    try:
        for key, data in rows_credential:
            locality_code_raw = data.get(b'INFO:LOCALITY_CODE')
            locality_code = locality_code_raw.decode('utf-8') if locality_code_raw else None
            if locality_code == '':
                continue
            if data.get(b'INFO:VALID_FROM') is None or data.get(b'INFO:VALID_FROM') == b'':
                continue
            valid_from = float(str(data.get(b'INFO:VALID_FROM').decode('utf-8'))) / 1000
            if data.get(b'INFO:VALID_TO') is None or data.get(b'INFO:VALID_TO') == b'':
                continue
            item_el = AccumulateCert(locality_code=locality_code)
            valid_to = float(str(data.get(b'INFO:VALID_TO').decode('utf-8'))) / 1000
            # check valid
            check_valid = int(valid_to > tm_end_date)
            check_year_create = int(tm_start_year <= valid_from < tm_end_date)
            check_month_create = int(tm_start_month <= valid_from < tm_end_date)
            check_date_create = int(tm_start_date <= valid_from < tm_end_date)
            # check expired
            check_year_expired = int(tm_start_year <= valid_to < tm_end_year)
            check_month_expired = int(tm_start_month <= valid_to < tm_end_month)
            check_date_expired = int(tm_start_date <= valid_to < tm_end_date)
            status = data.get(b'INFO:STATUS').decode('utf-8')
            pricing_code = data.get(b'INFO:PRICING_CODE').decode('utf-8')
            if status == "0":
                # pm valid and expired
                if pricing_code not in pricing_codes_ps0:
                    if check_valid == 1:
                        item_el.total = 1
                        item_el.total_pm = 1
                        item_el.total_year = check_year_create
                        item_el.total_pm_year = check_year_create
                        item_el.total_month = check_month_create
                        item_el.total_pm_month = check_month_create
                        item_el.total_date = check_date_create
                        item_el.total_pm_date = check_date_create
                    else:
                        item_el.total_year_expired = check_year_expired
                        item_el.total_pm_year_expired = check_year_expired
                        item_el.total_month_expired = check_month_expired
                        item_el.total_pm_month_expired = check_month_expired
                        item_el.total_date_expired = check_date_expired
                        item_el.total_pm_date_expired = check_date_expired
                # ps expired
                else:
                    if check_valid == 0:
                        item_el.total_year_expired = check_year_create
                        item_el.total_ps_year_expired = check_year_create
                        item_el.total_month_expired = check_month_create
                        item_el.total_ps_month_expired = check_month_create
                        item_el.total_date_expired = check_date_create
                        item_el.total_ps_date_expired = check_date_create
                    else:
                        credential_ps.append(key.decode('utf-8'))
            source = data.get(b'INFO:SOURCE').decode('utf-8')
            if source == "4":
                item_el.total_vneid = 1
                item_el.total_vneid_date = check_date_create
            elif source != "3":
                item_el.total_online = 1
                item_el.total_online_date = check_date_create
            allData.append(item_el)

    except Exception as e:
        print(f"Lỗi trong quá trình xử lý bản ghi: {str(e)}")
        traceback.print_exc()
        raise
    # endregion

    # region Ps tạo mới lũy kế và CTS gia hạn
    table_cert = hbase_connection.table('CERT')
    filter1 = "SingleColumnValueFilter('REQUEST', 'STATUS', =, 'binary:5')"
    filter2 = "SingleColumnValueFilter('REQUEST', 'REQUEST_TYPE', =, 'binary:0')"
    filter3 = "SingleColumnValueFilter('REQUEST', 'REQUEST_TYPE', =, 'binary:1')"

    # Kết hợp bộ lọc đúng cách
    filters = f"({filter1}) AND (({filter2}) OR ({filter3}))"
    rows_cert = table_cert.scan(filter=filters)

    try:
        for key, data in rows_cert:
            pricing_code_raw = data.get(b'REQUEST:PRICING_CODE')
            pricing_code = pricing_code_raw.decode('utf-8') if pricing_code_raw else None
            locality_code_raw = data.get(b'REQUEST:LOCALITY_CODE')
            locality_code = locality_code_raw.decode('utf-8') if locality_code_raw else None
            if locality_code == '':
                continue
            created_date_str = data.get(b'REQUEST:CREATED_DATE')
            if created_date_str is None or created_date_str == b'' or created_date_str == b'0':
                continue
            created_date = float(str(created_date_str.decode('utf-8'))) / 1000
            updated_date_str = data.get(b'REQUEST:UPDATED_TIME')
            if updated_date_str is None or updated_date_str == b'' or created_date_str == b'0':
                continue
            updated_date = float(str(updated_date_str.decode('utf-8'))) / 1000
            request_type = data.get(b'REQUEST:REQUEST_TYPE').decode('utf-8')
            credential_id = data.get(b'REQUEST:CREDENTIAL_ID').decode('utf-8')
            # check valid
            check_year_create = int(tm_start_year <= created_date < tm_end_date)
            check_month_create = int(tm_start_month <= created_date or updated_date < tm_end_date)
            check_date_create = int(tm_start_date <= created_date or updated_date < tm_end_date)
            item_el = AccumulateCert(locality_code=locality_code)
            # ps new and extend
            if pricing_code in pricing_codes_ps0:
                if request_type == "0" and pricing_code == "17187":
                    if credential_id in credential_ps:
                        item_el.total = 1
                        item_el.total_ps = 1
                        item_el.total_year = check_year_create
                        item_el.total_ps_year = check_year_create
                        item_el.total_month = check_month_create
                        item_el.total_ps_month = check_month_create
                        item_el.total_date = check_date_create
                        item_el.total_ps_date = check_date_create
                else:
                    item_el.total_extend = 1
                    item_el.total_ps_extend = 1
                    item_el.total_year_extend = check_year_create
                    item_el.total_ps_year_extend = check_year_create
                    item_el.total_month_extend = check_month_create
                    item_el.total_ps_month_extend = check_month_create
                    item_el.total_date_extend = check_date_create
                    item_el.total_ps_date_extend = check_date_create
            # pm extend
            elif request_type == "1":
                item_el.total_extend = 1
                item_el.total_pm_extend = 1
                item_el.total_year_extend = check_year_create
                item_el.total_pm_year_extend = check_year_create
                item_el.total_month_extend = check_month_create
                item_el.total_pm_month_extend = check_month_create
                item_el.total_date_extend = check_date_create
                item_el.total_pm_date_extend = check_date_create
            allData.append(item_el)

    except Exception as e:
        print(f"Lỗi trong quá trình xử lý bản ghi: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    # endregion

    if len(allData) < 1:
        return "Không có bản ghi phù hợp"
    values = [obj.__dict__ for obj in allData[0:]]
    # Chuyển đổi dữ liệu thành DataFrame
    df = pd.DataFrame(values)

    # Nhóm dữ liệu theo ClientId và tính tổng số lượt ký
    grouped = df.groupby(['locality_code']).agg({
        'total': 'sum',
        'total_pm': 'sum',
        'total_ps': 'sum',
        'total_year': 'sum',
        'total_pm_year': 'sum',
        'total_ps_year': 'sum',
        'total_month': 'sum',
        'total_pm_month': 'sum',
        'total_ps_month': 'sum',
        'total_date': 'sum',
        'total_pm_date': 'sum',
        'total_ps_date': 'sum',
        'total_extend': 'sum',
        'total_pm_extend': 'sum',
        'total_ps_extend': 'sum',
        'total_year_extend': 'sum',
        'total_pm_year_extend': 'sum',
        'total_ps_year_extend': 'sum',
        'total_month_extend': 'sum',
        'total_pm_month_extend': 'sum',
        'total_ps_month_extend': 'sum',
        'total_date_extend': 'sum',
        'total_pm_date_extend': 'sum',
        'total_ps_date_extend': 'sum',
        'total_year_expired': 'sum',
        'total_pm_year_expired': 'sum',
        'total_ps_year_expired': 'sum',
        'total_month_expired': 'sum',
        'total_pm_month_expired': 'sum',
        'total_ps_month_expired': 'sum',
        'total_date_expired': 'sum',
        'total_pm_date_expired': 'sum',
        'total_ps_date_expired': 'sum',
        'total_vneid': 'sum',
        'total_vneid_date': 'sum',
        'total_online': 'sum',
        'total_online_date': 'sum',

    }).reset_index()

    # region Lưu dữ liệu vào file Parquet
    table_parquet = grouped
    table_parquet['date_report'] = datetime.now()
    table_parquet = table_parquet[[
        'date_report',
        'locality_code',
        'total',
        'total_pm',
        'total_ps',
        'total_year',
        'total_pm_year',
        'total_ps_year',
        'total_month',
        'total_pm_month',
        'total_ps_month',
        'total_date',
        'total_pm_date',
        'total_ps_date',
        'total_extend',
        'total_pm_extend',
        'total_ps_extend',
        'total_year_extend',
        'total_pm_year_extend',
        'total_ps_year_extend',
        'total_month_extend',
        'total_pm_month_extend',
        'total_ps_month_extend',
        'total_date_extend',
        'total_pm_date_extend',
        'total_ps_date_extend',
        'total_year_expired',
        'total_pm_year_expired',
        'total_ps_year_expired',
        'total_month_expired',
        'total_pm_month_expired',
        'total_ps_month_expired',
        'total_date_expired',
        'total_pm_date_expired',
        'total_ps_date_expired',
        'total_vneid',
        'total_vneid_date',
        'total_online',
        'total_online_date'

    ]]
    # Tạo schema cho file Parquet
    schema = pa.schema([
        ('date_report', pa.date32()),
        ('locality_code', pa.string()),
        ('total', pa.int32()),
        ('total_pm', pa.int32()),
        ('total_ps', pa.int32()),
        ('total_year', pa.int32()),
        ('total_pm_year', pa.int32()),
        ('total_ps_year', pa.int32()),
        ('total_month', pa.int32()),
        ('total_pm_month', pa.int32()),
        ('total_ps_month', pa.int32()),
        ('total_date', pa.int32()),
        ('total_pm_date', pa.int32()),
        ('total_ps_date', pa.int32()),
        ('total_extend', pa.int32()),
        ('total_pm_extend', pa.int32()),
        ('total_ps_extend', pa.int32()),
        ('total_year_extend', pa.int32()),
        ('total_pm_year_extend', pa.int32()),
        ('total_ps_year_extend', pa.int32()),
        ('total_month_extend', pa.int32()),
        ('total_pm_month_extend', pa.int32()),
        ('total_ps_month_extend', pa.int32()),
        ('total_date_extend', pa.int32()),
        ('total_pm_date_extend', pa.int32()),
        ('total_ps_date_extend', pa.int32()),
        ('total_year_expired', pa.int32()),
        ('total_pm_year_expired', pa.int32()),
        ('total_ps_year_expired', pa.int32()),
        ('total_month_expired', pa.int32()),
        ('total_pm_month_expired', pa.int32()),
        ('total_ps_month_expired', pa.int32()),
        ('total_date_expired', pa.int32()),
        ('total_pm_date_expired', pa.int32()),
        ('total_ps_date_expired', pa.int32()),
        ('total_vneid', pa.int32()),
        ('total_vneid_date', pa.int32()),
        ('total_online', pa.int32()),
        ('total_online_date', pa.int32())
    ])

    # Chuyển đổi DataFrame sang Table của PyArrow
    table_result = pa.Table.from_pandas(table_parquet, schema=schema)
    hdfs_client = init_hdfs_connection()
    mess = _save_file_hdfs(hdfs_client, parquet_file_name, table_result)
    if mess != "":
        return mess
    # endregion

    # region gửi file tele
    end_date_str = end_date.strftime("%d/%m/%Y")
    header_index = ["TTKD VNPT T/TP",
                    "Tổng sản lượng CTS đang hoạt động dịch vụ VNPT SmartCA",
                    f"Tổng sản lượng CTS đang hoạt động gói thuê bao hết ngày {end_date_str}",
                    f"Tổng sản lượng CTS đang hoạt động gói SmartCA PS hết ngày  {end_date_str}",
                    f"Sản lượng CTS PTM năm {end_date.year}",
                    f"Sản lượng CTS gói thuê bao PTM năm {end_date.year}",
                    f"Sản lượng CTS Gói SmartCA PS PTM năm {end_date.year}",
                    f"Sản lượng CTS PTM tháng {end_date.month}",
                    f"Sản lượng CTS gói thuê bao PTM tháng {end_date.month}",
                    f"Sản lượng CTS Gói SmartCA PS PTM tháng {end_date.month}",
                    f"Sản lượng CTS PTM trong ngày {end_date_str}",
                    f"Sản lượng CTS gói thuê bao PTM trong ngày {end_date_str}",
                    f"Sản lượng CTS gói SmartCA PS PTM trong ngày {end_date_str}",
                    f"Tổng sản lượng CTS gia hạn dịch vụ VNPT SmartCA",
                    f"Tổng sản lượng CTS gia hạn gói thuê bao hết ngày {end_date_str}",
                    f"Tổng sản lượng CTS gia hạn gói SmartCA PS hết ngày {end_date_str}",
                    f"Sản lượng CTS GH năm {end_date.year}",
                    f"Sản lượng CTS gói thuê bao GH năm {end_date.year}",
                    f"Sản lượng CTS Gói SmartCA PS GH năm {end_date.year}",
                    f"Sản lượng CTS GH tháng {end_date.month}",
                    f"Sản lượng CTS gói thuê bao GH tháng {end_date.month}",
                    f"Sản lượng CTS Gói SmartCA PS GH tháng {end_date.month}",
                    f"Sản lượng CTS gói thuê bao GH trong ngày {end_date_str}",
                    f"Sản lượng CTS gói SmartCA PS GH trong ngày {end_date_str}",
                    f"Sản lượng CTS GH trong ngày {end_date_str}",
                    f"Sản lượng CTS hết hạn trong năm {end_date.year}",
                    f"Sản lượng CTS gói thuê bao hết hạn trong năm {end_date.year}",
                    f"Sản lượng CTS Gói SmartCA PS hết hạn trong năm {end_date.year}",
                    f"Sản lượng CTS hết hạn trong tháng {end_date.month}",
                    f"Sản lượng CTS gói thuê bao hết hạn trong tháng {end_date.month}",
                    f"Sản lượng CTS Gói SmartCA PS hết hạn trong tháng {end_date.month}",
                    f"Sản lượng CTS hết hạn trong ngày {end_date_str}",
                    f"Sản lượng CTS gói thuê bao hết hạn trong ngày {end_date_str}",
                    f"Sản lượng CTS Gói SmartCA PS hết hạn trong ngày {end_date_str}",
                    f"Sản lượng CTS được đăng ký và khởi tạo từ nguồn VneId",
                    f"Sản lượng CTS được đăng ký và khởi tạo từ nguồn VneId trong ngày {end_date_str}",
                    f"Sản lượng CTS được đăng ký và khởi tạo online toàn trình",
                    f"Sản lượng CTS được đăng ký và khởi tạo online toàn trình trong ngày {end_date_str}"]
    grouped = grouped.drop('date_report', axis=1)
    grouped.columns = header_index
    await send_excel_to_telegram(excel_buffer=_convert_to_excel(grouped), file_name=file_name)
    # endregion
    hbase_connection.close()
    return ""


async def job_accumulate_credential():
    date_now = datetime.now()
    await processing_accumulate_credential(date_now)


async def processing_cert_order_register(date_now):
    hbase_connection = init_hbase_connection()
    hdfs_client = init_hdfs_connection()
    temp_time = f"{date_now.year}-{date_now.month}-01T17:00:00.000Z"
    end_date = datetime.strptime(temp_time, "%Y-%m-%dT%H:%M:%S.%fZ") - timedelta(days=1)
    start_date = end_date - relativedelta(months=1)
    parquet_file_name = f"warehouse/cert_order_register/cert_order_register_{end_date.year}.parquet"
    file_name = f"TrangThaiDonHangDangKy{end_date.year}{end_date.month:02}{end_date.day:02}"
    # Lấy dữ liệu từ HBase
    allData = []
    cert_order = []
    end_date_str = end_date.strftime("%d/%m/%Y")
    status_dict = _get_status_dictionary(hdfs_client)
    status_cert_order = []
    status_register = []
    for data in status_dict:
        if data['type'] != 'STATUS_DEFINITION':
            continue
        if data['table'] == 'CERT_ORDER':
            status_cert_order.append(data)
        if data['table'] == 'REGISTER':
            status_register.append(data)
    # region Order
    table_order = hbase_connection.table('CERT_ORDER')
    filter_order_start1 = f"SingleColumnValueFilter('INFO', 'CREATED_DATE', >=, 'binary:{int(start_date.timestamp() * 1000)}')"
    filter_order_end1 = f"SingleColumnValueFilter('INFO', 'CREATED_DATE', <, 'binary:{int(end_date.timestamp() * 1000)}')"
    filter_order_start2 = f"SingleColumnValueFilter('INFO', 'UPDATED_DATE', >=, 'binary:{int(start_date.timestamp() * 1000)}')"
    filter_order_end2 = f"SingleColumnValueFilter('INFO', 'UPDATED_DATE', <, 'binary:{int(end_date.timestamp() * 1000)}')"
    filters = f"({filter_order_start1} AND {filter_order_end1}) OR ({filter_order_start2} AND {filter_order_end2})"
    rows_order = table_order.scan(filter=filters)

    try:
        for key, data in rows_order:
            item = CertOrderRegister(end_date_str)
            cert_order.append(data.get(b'INFO:IDENTITY_ID').decode('utf-8'))
            item.ma_tinh = _check_status_string(data.get(b'INFO:LOCALITY_CODE').decode('utf-8'))
            item.sdt_lh = _check_status_string(data.get(b'INFO:PHONE').decode('utf-8'))
            item.ten_kh = _check_status_string(data.get(b'INFO:FULL_NAME').decode('utf-8'))
            item.so_gt = _check_status_string(data.get(b'INFO:UID').decode('utf-8'))
            item.ma_tb = _check_status_string(data.get(b'INFO:MA_TB').decode('utf-8'))
            item.ma_don_hang = _check_status_string(data.get(b'INFO:MA_GD').decode('utf-8'))
            item.dia_chi_ct = _check_status_string(data.get(b'INFO:ADDRESS').decode('utf-8'))
            created_date = datetime.fromtimestamp(
                timestamp=(float(str(data.get(b'INFO:CREATED_DATE').decode('utf-8'))) / 1000))
            if created_date != 'None' and created_date != b'' and created_date != b"":
                item.ngay_tao_don = (created_date + timedelta(hours=7)).strftime("%Y/%m/%d %H:%M:%S")
            log_created_date = data.get(b'INFO:LOG_CREATED_DATE').decode('utf-8')
            if log_created_date != 'None' and log_created_date != b'' and log_created_date != b"":
                item.ngay_thuc_hien = (
                        datetime.fromtimestamp(timestamp=(float(str(log_created_date)) / 1000)) + timedelta(hours=7)).strftime("%Y/%m/%d %H:%M:%S")
                item.nguyen_nhan = data.get(b'INFO:LOG_CONTENT').decode('utf-8')
            item.kenh_ban = _check_status_string(data.get(b'INFO:CLIENT_NAME').decode('utf-8'))
            item.loai_yeu_cau = _check_status_string(data.get(b'INFO:TYPE_DESC').decode('utf-8'))
            status = _check_status_string(data.get(b'INFO:STATUS').decode('utf-8'))
            status_desc = None
            if status != '':
                status_desc = next((entry for entry in status_cert_order if entry['code'] == int(status)), None)
            if status_desc is not None:
                item.ly_do_gd = status_desc['description_vi']
            else:
                item.ly_do_gd = _check_status_string(data.get(b'INFO:STATUS_DESC').decode('utf-8'))
            item.toc_do_id = _check_status_string(data.get(b'INFO:PRICING_CODE').decode('utf-8'))
            item.ten_goi_cuoc = _check_status_string(data.get(b'INFO:PRICING_NAME').decode('utf-8'))
            item.gia_goi_cuoc = _check_status_string(data.get(b'INFO:PRICING_PRICE').decode('utf-8'))
            item.trang_thai_tk = ""
            allData.append(item)
    except Exception as e:
        print(f"Lỗi trong quá trình xử lý bản ghi: {str(e)}")
        logger.error(traceback.format_exc())
        raise

    # endregion

    # region Province
    provinces = _get_data_locality(hdfs_client)
    # endregion

    # region Register
    table_register = hbase_connection.table('USER_INFO')
    filter_start = f"SingleColumnValueFilter('REGISTER', 'CREATE_DATE', >=, 'binary:{int(start_date.timestamp() * 1000)}')"
    filter_end = f"SingleColumnValueFilter('REGISTER', 'CREATE_DATE', <, 'binary:{int(end_date.timestamp() * 1000)}')"
    filters = f"{filter_start} AND {filter_end}"
    rows_register = table_register.scan(filter=filters)

    try:
        for key, data in rows_register:
            if key.decode('utf-8') in cert_order:
                continue
            item = CertOrderRegister(end_date_str)
            province_id = _check_status_string(data.get(b'REGISTER:PROVINCE_ID').decode('utf-8'))
            if province_id != '':
                item.ma_tinh = _check_province_code(provinces, province_id)
            item.sdt_lh = _check_status_string(data.get(b'REGISTER:PHONE').decode('utf-8'))
            item.ten_kh = _check_status_string(data.get(b'REGISTER:FULL_NAME').decode('utf-8'))
            item.so_gt = _check_status_string(data.get(b'REGISTER:UID').decode('utf-8'))
            item.dia_chi_ct = _check_status_string(data.get(b'REGISTER:ADDRESS').decode('utf-8'))
            created_date = datetime.fromtimestamp(
                timestamp=(float(str(_check_status_string(data.get(b'REGISTER:CREATE_DATE').decode('utf-8')))) / 1000))
            if created_date != 'None' and created_date != b'' and created_date != b"":
                item.ngay_tao_don = (created_date + timedelta(hours=7)).strftime("%Y/%m/%d %H:%M:%S")
            log_created_date = _check_status_string(data.get(b'REGISTER:MODIFIED_DATE').decode('utf-8'))
            if log_created_date != 'None' and log_created_date != b'' and log_created_date != b"":
                item.ngay_thuc_hien = _check_status_string(
                    (datetime.fromtimestamp(timestamp=(float(str(log_created_date)) / 1000)) +
                     timedelta(hours=7)).strftime("%Y/%m/%d %H:%M:%S"))
            else:
                item.ngay_thuc_hien = item.ngay_tao_don
            status = _check_status_string(data.get(b'REGISTER:STATUS').decode('utf-8'))
            status_desc = None
            if status != '':
                status_desc = next((entry for entry in status_register if entry['code'] == int(status)), None)
            if status_desc is not None:
                status = status_desc['description_vi']
            else:
                status = _check_status_string(data.get(b'REGISTER:STATUS_DESC').decode('utf-8'))
            item.ly_do_gd = status
            item.nguyen_nhan = status
            item.trang_thai_tk = status
            allData.append(item)
    except Exception as e:
        print(f"Lỗi trong quá trình xử lý bản ghi: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    # endregion

    if len(allData) < 1:
        return "Không có bản ghi phù hợp"
    values = [obj.__dict__ for obj in allData[0:]]
    # Chuyển đổi dữ liệu thành DataFrame
    df = pd.DataFrame(values)
    grouped = df.reset_index(drop=True)
    table_parquet = grouped
    table_parquet = table_parquet[[
        'ngay_lay_dl',
        'ma_tinh',
        'sdt_lh',
        'ten_kh',
        'so_gt',
        'ma_tb',
        'ma_don_hang',
        'dia_chi_ct',
        'ngay_tao_don',
        'ngay_thuc_hien',
        'kenh_ban',
        'loai_yeu_cau',
        'ly_do_gd',
        'nguyen_nhan',
        'toc_do_id',
        'ten_goi_cuoc',
        'gia_goi_cuoc',
        'trang_thai_tk'
    ]]
    schema = pa.schema([
        ('ngay_lay_dl', pa.string()),
        ('ma_tinh', pa.string()),
        ('sdt_lh', pa.string()),
        ('ten_kh', pa.string()),
        ('so_gt', pa.string()),
        ('ma_tb', pa.string()),
        ('ma_don_hang', pa.string()),
        ('dia_chi_ct', pa.string()),
        ('ngay_tao_don', pa.string()),
        ('ngay_thuc_hien', pa.string()),
        ('kenh_ban', pa.string()),
        ('loai_yeu_cau', pa.string()),
        ('ly_do_gd', pa.string()),
        ('nguyen_nhan', pa.string()),
        ('toc_do_id', pa.string()),
        ('ten_goi_cuoc', pa.string()),
        ('gia_goi_cuoc', pa.string()),
        ('trang_thai_tk', pa.string()),
    ])
    # Chuyển đổi DataFrame sang Table của PyArrow
    table_result = pa.Table.from_pandas(table_parquet, schema=schema)

    # Lưu dữ liệu vào file Parquet
    mess = _save_file_hdfs(hdfs_client, parquet_file_name, table_result)
    if mess != "":
        return mess
    # region gửi file tele

    header_index = [
        'Ngày lấy dữ liệu',
        'Mã tỉnh',
        'Số điện thoại',
        'Họ và tên',
        'Số giấy tờ',
        'Mã thuê bao',
        'Mã đơn hàng',
        'Địa chỉ chi tiết',
        'Ngày tạo đơn',
        'Ngày thực hiện',
        'Kênh bán',
        'Loại yêu cầu',
        'Lý do giao dịch',
        'Nguyên nhân',
        'Tốc độ ID',
        'Tên gói cước',
        'Giá gói cước',
        'Trạng thái tài khoản'
    ]
    grouped.columns = header_index
    await send_excel_to_telegram(excel_buffer=_convert_to_excel(grouped), file_name=file_name)
    # endregion
    hbase_connection.close()
    return ""


async def job_cert_order_register():
    date_now = datetime.now()
    await processing_cert_order_register(date_now)


def _save_file_hdfs(hdfs_client, file_path, table_result):
    try:
        buffer = io.BytesIO()
        pq.write_table(table_result, buffer)
        buffer.seek(0)
        if hdfs_client.status(file_path, strict=False) is not None:
            with hdfs_client.read(file_path) as existing_file:
                buffer_existing = io.BytesIO(existing_file.read())
                existing_table = pq.read_table(buffer_existing)
            combined_table = pa.concat_tables([existing_table, table_result])
            buffer_combined = io.BytesIO()
            pq.write_table(combined_table, buffer_combined)
            buffer_combined.seek(0)

            with hdfs_client.write(file_path, overwrite=True) as final_file:
                final_file.write(buffer_combined.getvalue())
                final_file.flush()
        else:
            with hdfs_client.write(file_path, overwrite=True) as hdfs_file:
                hdfs_file.write(buffer.getvalue())
                hdfs_file.flush()
        print(f"File {file_path} đã được ghi thành công lên HDFS!")
    except Exception as e:
        mess = f"Lỗi khi ghi tệp Parquet: {str(e)}"
        print(mess)
        logger.error(traceback.format_exc())
        return mess
    return ""


def _convert_to_excel(df):
    # Tạo một buffer (file trong bộ nhớ)
    excel_buffer = io.BytesIO()
    df.to_excel(excel_buffer, index=False, engine='openpyxl')
    excel_buffer.seek(0)
    return excel_buffer


async def send_excel_to_telegram(excel_buffer, file_name):
    try:
        access_token, chat_id = init_connect_tele()
        bot = Bot(token=access_token)
        # Tạo InputFile từ buffer
        excel_buffer.name = f"{file_name}.xlsx"  # Cung cấp tên cho file khi gửi
        await bot.send_document(chat_id=chat_id, document=InputFile(excel_buffer))
    except Exception as e:
        print(f"Lỗi khi gửi file telegram. {str(e)}")
        logger.error(traceback.format_exc())


def _check_status_string(string):
    if string == "None" or string is None or string == "" or string == '':
        return ""
    else:
        return string


def _check_province_code(provinces, province_id):
    if province_id == "None" or province_id is None or province_id == "" or province_id == '' or not str.isdigit(
            province_id):
        return province_id
    else:
        if int(province_id) in provinces['province_id'].values:
            return provinces.loc[provinces['province_id'] == int(province_id), 'province_code'].values[0]
        return ""


def _query_parquet_by_field(hdfs_client, file_path, field_name, filter_value):
    try:
        with hdfs_client.read(file_path) as existing_file:
            buffer_existing = io.BytesIO(existing_file.read())
            table = pq.read_table(buffer_existing)
            df = table.to_pandas()
        if field_name is not None and filter_value is not None:
            filtered_df = df[df[field_name] == filter_value]
        else:
            filtered_df = df
        return filtered_df.to_dict(orient='records')
    except Exception as e:
        print(f"Lỗi khi truy vấn file Parquet: {str(e)}")
        logger.error(traceback.format_exc())
        return None


def _get_data_locality(hdfs_client):
    try:
        file_path = "/data_cut_off.parquet"
        list_file = _query_parquet_by_field(hdfs_client, file_path, "table_name", "Locality")
        list_file = sorted(list_file, key=lambda x: x["date_report"], reverse=True)
        file_name = list_file[0]["file_name"]
        with hdfs_client.read(file_name) as hdfs_file:
            buffer_existing = io.BytesIO(hdfs_file.read())
            table = pq.read_table(buffer_existing)
            df = table.to_pandas()
        return df
    except Exception as e:
        print(f"Lỗi khi truy vấn file Parquet: {str(e)}")
        logger.error(traceback.format_exc())
        return None


def _get_status_dictionary(hdfs_client):
    try:
        file_path = "definition/refer_vi.parquet"
        df = _query_parquet_by_field(hdfs_client, file_path, None, None)
        return df
    except Exception as e:
        print(f"Lỗi khi truy vấn file Parquet: {str(e)}")
        logger.error(traceback.format_exc())
        return None
