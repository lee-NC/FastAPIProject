import asyncio
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import logging
from Helper.config import Config, init_connect

CHUNK_SIZE = 1000
MAX_WORKERS = 4

logger = logging.getLogger("Lakehouse")

config = Config()


async def process_fetch_tables():
    date_now = datetime.now()
    temp_time = f"{date_now.year}-{date_now.month}-{date_now.day - 1}T17:00:00.000Z"
    end_date = datetime.strptime(temp_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    start_date = end_date - timedelta(days=1)
    await load_data_identity(start_date, end_date)


async def load_data_identity(start_date, end_date):
    try:
        node_name = "signservice_identity"
        await load_personal_turn_order(node_name, start_date, end_date)
    except Exception as e:
        logger.error(f"Bỏ qua bảng do lỗi: {e}")
        traceback.print_exc()


async def load_personal_turn_order(node_name, start_date, end_date):
    collection, table, hbase_connection, mongo_client = init_connect(config, node_name, "PersonalSignTurnOrder", ["PERSONAL_SIGN_TURN_ORDER"])
    try:
        documents = collection.find({'$and': [
            {'$or': [{'CreatedDate': {'$gte': start_date}}, {'UpdatedDate': {'gte': start_date}}]},
            {'CreatedDate': {'$lt': end_date}}]},
            no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(table, "PersonalSignTurnOrder", documents)
    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng PersonalSignTurnOrder: {e}")
        traceback.print_exc()
    finally:
        hbase_connection.close()
        mongo_client.close()
    logger.info("Chuyển dữ liệu PersonalSignTurnOrder thành công từ MongoDB sang HBase.")


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
        logger.error(f"Lỗi khi xử lý bảng {collection_name}: {e}")
        traceback.print_exc()


async def _transfer_chunk_sync(chunk, table, collection_name):
    try:
        match collection_name:
            case "PersonalSignTurnOrder":
                await _transfer_personal_sign_turn_order(chunk, table)
            case _:
                logger.info(f"Bảng {collection_name} không được nhận diện.")
                return None
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý chunk: {e}")


async def _transfer_personal_sign_turn_order(chunk, table):
    try:
        batch = table[0].batch()
        for document in chunk:
            row_key = str(document["_id"])

            Status = document.get("Status", None)
            StatusDesc = document.get("StatusDesc", "")
            UpdatedDate = document.get("UpdatedDate", None)

            PaymentStatus = document.get("PaymentStatus", None)
            PaymentStatusDesc = document.get("PaymentStatuDesc", "")
            TotalMoney = document.get("TotalMoney", None)
            if TotalMoney is not None:
                TotalMoney = str(TotalMoney)  # Chuyển Decimal về chuỗi

            Pricings = document.get('Pricings', [])
            if Pricings:
                Pricing = Pricings[-1]
                PricingName = Pricing.get("Name", "")
                PricingCode = Pricing.get("tocdo_id", "")
                Code = Pricing.get("Code", "")
                SignTurnNumber = Pricing.get("SignTurnNumber", 0)
            else:
                PricingName = ""
                PricingCode = ""
                Code = ""
                SignTurnNumber = 0

            old_item = table[0].row(row_key)
            if old_item is None or old_item == {}:
                Indentity = document.get('UserInfo', {})
                IdentityId = Indentity.get('_id', '')
                Uid = Indentity.get("Uid", "")
                FullName = Indentity.get("FullName", "")
                LocalityCode = Indentity.get("LocalityCode", "")
                CreatedDate = document.get("CreatedDate", None)

                DHSXKDCustomerInfo = document.get("DHSXKDCustomerInfo", {})
                ma_tb = DHSXKDCustomerInfo.get("ma_tb", "")
                ma_gd = DHSXKDCustomerInfo.get("ma_gd", "")
                ma_kh = DHSXKDCustomerInfo.get("ma_kh", "")
                ma_hd = DHSXKDCustomerInfo.get("ma_hd", "")
                ma_hrm = DHSXKDCustomerInfo.get("ma_hrm", "")

                CredentialId = document.get("CredentialId", "")
                PaymentOrderId = document.get("PaymentOrderId", "")
                IsSyncDHSXKD = int(document.get("IsSyncDHSXKD", False))
                MaGt = document.get("MaGt", "")

                # Ghi vào HBase
                table[0].put(row_key, {
                    "INFO:IDENTITY_ID": str(IdentityId).encode('utf-8'),
                    "INFO:UID": str(Uid).encode('utf-8'),
                    "INFO:FULL_NAME": str(FullName).encode('utf-8'),
                    "INFO:LOCALITY_CODE": str(LocalityCode).encode('utf-8'),
                    "INFO:STATUS": str(Status).encode('utf-8') if Status is not None else b"",
                    "INFO:STATUS_DESC": str(StatusDesc).encode('utf-8'),
                    "INFO:CREATED_DATE": str(CreatedDate).encode('utf-8') if CreatedDate else b"",
                    "INFO:UPDATED_DATE": str(UpdatedDate).encode('utf-8') if UpdatedDate else b"",
                    "INFO:MA_TB": str(ma_tb).encode('utf-8'),
                    "INFO:MA_GD": str(ma_gd).encode('utf-8'),
                    "INFO:MA_KH": str(ma_kh).encode('utf-8'),
                    "INFO:MA_HD": str(ma_hd).encode('utf-8'),
                    "INFO:MA_HRM": str(ma_hrm).encode('utf-8'),
                    "INFO:CREDENTIAL_ID": str(CredentialId).encode('utf-8'),
                    "INFO:PAYMENT_ORDER_ID": str(PaymentOrderId).encode('utf-8'),
                    "INFO:PAYMENT_STATUS": str(PaymentStatus).encode('utf-8') if PaymentStatus is not None else b"",
                    "INFO:PAYMENT_STATUS_DESC": str(PaymentStatusDesc).encode('utf-8'),
                    "INFO:IS_SYNC_DHSXKD": str(IsSyncDHSXKD).encode('utf-8'),
                    "INFO:MA_GT": str(MaGt).encode('utf-8'),
                    "INFO:PRICING_NAME": str(PricingName).encode('utf-8'),
                    "INFO:PRICING_CODE": str(PricingCode).encode('utf-8'),
                    "INFO:CODE": str(Code).encode('utf-8'),
                    "INFO:SIGN_TURN_NUMBER": str(SignTurnNumber).encode('utf-8'),
                    "INFO:TOTAL_MONEY": str(TotalMoney).encode('utf-8') if TotalMoney is not None else b""
                })
            else:
                # Cập nhật các trường nếu có thay đổi
                if _column_value_exists(table, row_key, "info", "status", Status):
                    table[0].put(row_key, {'info:status': str(Status).encode('utf-8')})
                if _column_value_exists(table, row_key, "info", "status_desc", StatusDesc):
                    table[0].put(row_key, {'info:status_desc': str(StatusDesc).encode('utf-8')})
                if _column_value_exists(table, row_key, "info", "updated_date", UpdatedDate):
                    table[0].put(row_key, {'info:updated_date': str(UpdatedDate).encode('utf-8')})
                if _column_value_exists(table, row_key, "info", "payment_status", PaymentStatus):
                    table[0].put(row_key, {'info:payment_status': str(PaymentStatus).encode('utf-8')})
                if _column_value_exists(table, row_key, "info", "payment_status_desc", PaymentStatusDesc):
                    table[0].put(row_key, {'info:payment_status_desc': str(PaymentStatusDesc).encode('utf-8')})
                if _column_value_exists(table, row_key, "info", "pricing_name", PricingName):
                    table[0].put(row_key, {'info:pricing_name': str(PricingName).encode('utf-8')})
                if _column_value_exists(table, row_key, "info", "pricing_code", PricingCode):
                    table[0].put(row_key, {'info:pricing_code': str(PricingCode).encode('utf-8')})
                if _column_value_exists(table, row_key, "info", "code", Code):
                    table[0].put(row_key, {'info:code': str(Code).encode('utf-8')})
                if _column_value_exists(table, row_key, "info", "sign_turn_number", SignTurnNumber):
                    table[0].put(row_key, {'info:sign_turn_number': str(SignTurnNumber).encode('utf-8')})
                if _column_value_exists(table, row_key, "info", "total_money", TotalMoney):
                    table[0].put(row_key, {'info:total_money': str(TotalMoney).encode('utf-8')})
        batch.send()
        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {e}")
        traceback.print_exc()


def _column_value_exists(table, row_key, column_family, column_name, new_value):
    # Lấy giá trị hiện tại của cột
    current_value = table[0].row(row_key).get(f'{column_family}:{column_name}')
    check = current_value is not None and current_value != new_value and current_value != ''
    # So sánh giá trị hiện tại với giá trị mới
    return check
