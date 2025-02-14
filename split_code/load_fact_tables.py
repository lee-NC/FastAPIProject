import asyncio
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import logging

from helper.get_config import init_connect_mongo, init_connect_hbase

CHUNK_SIZE = 4000
MAX_WORKERS = 4

logger = logging.getLogger("Lakehouse")


async def process_fetch_tables():
    date_now = datetime.now()
    temp_time = f"{date_now.year}-{date_now.month}-{date_now.day - 1}T17:00:00.000Z"
    end_date = datetime.strptime(temp_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    start_date = end_date - timedelta(days=1)
    await load_data_csc(start_date, end_date)


async def load_data_csc(start_date, end_date):
    try:
        node_name = "signservice_credential"
        await load_cert(node_name, start_date, end_date)
    except Exception as e:
        logger.error(f"Bỏ qua bảng do lỗi: {str(e)}")
        logger.error(traceback.format_exc())


async def load_cert(node_name, start_date, end_date):
    collection_request_cert, mongo_client = init_connect_mongo(node_name, "RequestCert")
    table = ["CERT"]
    try:
        documents_request_cert = collection_request_cert.find({'$and': [
            {'$or': [{'createdDate': {'$gte': start_date}}, {'updatedTime': {'gte': start_date}}]},
            {'createdDate': {'$lt': end_date}}]},
            no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(table, "RequestCert", documents_request_cert)
    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng RequestCert: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        mongo_client.close()
    logger.info("Chuyển dữ liệu RequestCert thành công từ MongoDB sang HBase.")


async def _process_chunks(table, collection_name, documents):
    try:
        chunks = []
        chunk = []
        i = 0
        for i, document in enumerate(documents):
            chunk.append(document)
            if (i + 1) % CHUNK_SIZE == 0:
                chunks.append(chunk)
                chunk = []
        if chunk:
            chunks.append(chunk)
        documents.close()
        # Xử lý các chunks song song
        with ThreadPoolExecutor(max_workers=MAX_WORKERS):
            tasks = [
                await asyncio.to_thread(_transfer_chunk_sync, chunk, table, collection_name)
                for chunk in chunks
            ]
            if tasks:
                await asyncio.gather(*tasks)

        logger.info(f"Chuyển {i + 1} bản ghi {collection_name} thành công từ MongoDB sang HBase.")
    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng {collection_name}: {str(e)}")
        logger.error(traceback.format_exc())


async def _transfer_chunk_sync(chunk, table, collection_name):
    try:
        if collection_name == "RequestCert":
            await _transfer_request_cert(chunk, table)
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý chunk: {str(e)}")
        logger.error(traceback.format_exc())


async def _transfer_request_cert(chunk, table_names):
    keyword = "OPER"
    try:
        tables, hbase_connection = init_connect_hbase(table_names)
        table = tables[0]
        batch = table.batch()
        for document in chunk:
            row_key = str(document["_id"])
            status = _convert_number(document.get("status", ""))
            statusDesc = str(document.get("statusDesc", ""))
            updatedTime = _convert_datetime(document.get("updatedTime", ""))
            approveTime = _convert_datetime(document.get("approveTime", ""))
            credentialId = str(document.get("credentialId", ""))

            clientId = str(document.get("createdByClientId", ""))
            clientName = str(document.get("createdByClientName", "")).encode('utf-8')
            createdDate = _convert_datetime((document.get("createdDate", "")))
            pricingCode = str(document.get("pricingCode", ""))
            pricingName = str(document.get("pricingName", "")).encode('utf-8')
            username = str(document.get("username", ""))
            localityCode = ""
            if username != "":
                index = username.find(keyword)
                if index != -1:
                    localityCode = username[index:index + 3]
                else:
                    localityCode = username[3:6]
            logger.info(f"username: {username}, localityCode:{localityCode}")
            uid = str(document.get("uid", ""))
            identityId = str(document.get("identityId", ""))
            fullName = str(document.get("fullName", "")).encode('utf-8')
            period = str(document.get("period", ""))
            code = str(document.get("code", ""))
            requestType = _convert_number(document.get("requestType", ""))
            requestTypeDesc = str(document.get("requestTypeDesc", ""))
            # Ghi vào HBase
            table.put(row_key, {
                "REQUEST:CREDENTIAL_ID": credentialId,
                "REQUEST:STATUS": status,
                "REQUEST:STATUS_DESC": statusDesc,
                "REQUEST:CLIENT_ID": clientId,
                "REQUEST:CLIENT_NAME": clientName,
                "REQUEST:UPDATED_TIME": updatedTime,
                "REQUEST:CREATED_DATE": createdDate,
                "REQUEST:PRICING_CODE": pricingCode,
                "REQUEST:PRICING_NAME": pricingName,
                "REQUEST:USERNAME": username,
                "REQUEST:LOCALITY_CODE": localityCode,
                "REQUEST:UID": uid,
                "REQUEST:IDENTITY_ID": identityId,
                "REQUEST:FULL_NAME": fullName,
                "REQUEST:PERIOD": period,
                "REQUEST:CODE": code,
                "REQUEST:APPROVE_TIME": approveTime,
                "REQUEST:REQUEST_TYPE": requestType,
                "REQUEST:REQUEST_TYPE_DESC": requestTypeDesc
            })

        batch.send()
        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        hbase_connection.close()


def _column_value_exists(table, row_key, column_family, column_name, new_value):
    # Lấy giá trị hiện tại của cột
    current_value = table.row(row_key).get(f'{column_family}:{column_name}')
    check = current_value is not None and current_value != new_value and current_value != ''
    # So sánh giá trị hiện tại với giá trị mới
    return check


def _convert_datetime(input_datetime):
    if input_datetime is not None and input_datetime != "" and input_datetime != datetime(1, 1, 1, 0, 0):
        return str(int(input_datetime.timestamp() * 1000))
    return b''


def _convert_number(input_int):
    if input_int is not None and input_int != "":
        return str(input_int)
    return b''


def _convert_boolean(input_boolean):
    if input_boolean is not None and input_boolean != "":
        return str(int(input_boolean))
    return b''
