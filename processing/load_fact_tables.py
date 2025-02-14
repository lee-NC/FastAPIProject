import asyncio
import re
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
    await load_data_identity(start_date, end_date)
    await load_data_csc(start_date, end_date)


async def load_data_identity(start_date, end_date):
    try:
        node_name = "signservice_identity"
        await load_user(node_name, start_date, end_date)
        await load_cert_order(node_name, start_date, end_date)
        await load_personal_turn_order(node_name, start_date, end_date)
    except Exception as e:
        logger.error(f"Bỏ qua bảng do lỗi: {str(e)}")
        logger.error(traceback.format_exc())


async def load_cert_order(node_name, start_date, end_date):
    collection, mongo_client = init_connect_mongo(node_name, "CertOrder")
    table = ["CERT_ORDER"]
    try:
        documents = collection.find({'$and': [
            {'$or': [{'CreatedDate': {'$gte': start_date}}, {'UpdatedDate': {'gte': start_date}}]},
            {'CreatedDate': {'$lt': end_date}}]},
            no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(table, "CertOrder", documents)
    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng CertOrder: {str(e)} ")
        logger.error(traceback.format_exc())
    finally:
        mongo_client.close()
    logger.info("Chuyển dữ liệu CertOrder thành công từ MongoDB sang HBase.")


async def _transfer_cert_order(chunk, table_names):
    tables, hbase_connection = init_connect_hbase(table_names)
    table = tables[0]
    batch = table.batch()
    try:
        for document in chunk:
            full_name = document.get("FullName", "")
            if full_name and "test" in full_name.lower():
                continue
            pricing = document.get("Pricing", {})
            if pricing is None or pricing is {}:
                continue
            pricing_name = pricing.get("PricingName", "")
            if pricing_name and "test" in pricing_name.lower():
                continue
            dhsxkd_customer_info = document.get("DHSXKDCustomerInfo", {})
            if dhsxkd_customer_info is None or dhsxkd_customer_info is {}:
                continue
            ma_gd = dhsxkd_customer_info.get("ma_gd", "")
            if ma_gd and "test" in ma_gd.lower():
                continue
            row_key = str(document["_id"])
            status = _convert_number(document.get("Status", ""))
            status_desc = document.get("StatusDesc", "")
            log_content = None
            log_created_date = None
            updated_date = ""
            updated_date_check = document.get("UpdatedDate", "")
            if updated_date_check is not None and updated_date_check != "" and updated_date_check != datetime(1, 1, 1, 0,
                                                                                                              0):
                updated_date = _convert_datetime(updated_date_check)
                if status is not None and 50 <= int(status) < 99:
                    Logs = document.get("Logs", [])
                    if Logs is not None and Logs is not {} and Logs:
                        for record in reversed(Logs):
                            if record.get("IsError", "") == "True" or record.get("IsError", "") == "true" or record.get(
                                    "IsError", "") == "1":
                                log_content = record.get("Content", "")
                                log_created_date = _convert_datetime(record.get("CreatedDate", ""))
                                break

            credential_id = document.get("CredentialId", "")
            request_cert_id = document.get("RequestCertId", "")
            contract_url = document.get("ContractUrl", "")
            serial = document.get("Serial", "")
            acceptance_url = ""
            acceptance = document.get('AcceptanceDocuments', [])
            if acceptance is not None and acceptance is not [] and acceptance:
                acceptance_url = acceptance[-1].get('UrlSigned', '') if document.get(
                    'AcceptanceDocuments') else ''

            old_item = table.row(row_key)
            if old_item is None or old_item == {}:
                identity_id = str(document.get("IdentityId", ""))
                uid = document.get("Uid", "")

                email = document.get("Email", "")
                phone = document.get("Phone", "")
                locality_code = document.get("LocalityCode", "")
                type = _convert_number(document.get("Type", ""))
                type_desc = document.get("TypeDesc", "")

                client_name = document.get("ClientName", "")
                ClientId = document.get("ClientId", "")
                CreatedDate = _convert_datetime(document.get("CreatedDate", ""))

                ma_tb = dhsxkd_customer_info.get("ma_tb", "")

                ma_kh = dhsxkd_customer_info.get("ma_kh", "")
                ma_hd = dhsxkd_customer_info.get("ma_hd", "")
                ma_hrm = dhsxkd_customer_info.get("ma_hrm", "")
                # address
                Source = _convert_number(document.get("Source", ""))
                # serial

                PricingCode = pricing.get("PricingCode", "")

                Price = pricing.get("Price", "")
                Code = pricing.get("Code", "")
                SignType = _convert_number(pricing.get("SignType", ""))
                Validity = _convert_number(pricing.get("Validity", ""))
                MaGt = document.get("MaGt", "")

                PreviousSerial = document.get("PreviousSerial", "")

                if document.get('Address'):
                    identity_address = document.get('Address', {})
                    provinceId = _convert_number(identity_address.get("provinceId", ""))
                    provinceName = identity_address.get("provinceName", "")
                    districtId = _convert_number(identity_address.get("districtId", ""))
                    districtName = identity_address.get("districtName", "")
                    wardId = _convert_number(identity_address.get("wardId", ""))
                    wardName = identity_address.get("wardName", "")
                    streetName = identity_address.get("streetName", "")
                    address = identity_address.get("address", "")
                else:
                    provinceId = ""
                    provinceName = ""
                    districtId = ""
                    districtName = ""
                    wardId = ""
                    wardName = ""
                    streetName = ""
                    address = ""
                # Ghi vào HBase
                data = {
                    "INFO:IDENTITY_ID": identity_id,
                    "INFO:UID": str(uid),
                    "INFO:FULL_NAME": str(full_name).encode('utf-8'),
                    "INFO:EMAIL": str(email),
                    "INFO:PHONE": str(phone),
                    "INFO:LOCALITY_CODE": str(locality_code),
                    "INFO:TYPE": str(type) if type is not None else None,
                    "INFO:TYPE_DESC": str(type_desc),
                    "INFO:STATUS": str(status) if status is not None else None,
                    "INFO:STATUS_DESC": str(status_desc),
                    "INFO:CLIENT_NAME": str(client_name).encode('utf-8'),
                    "INFO:CLIENT_ID": str(ClientId),
                    "INFO:CREATED_DATE": str(CreatedDate),
                    "INFO:UPDATED_DATE": str(updated_date) if updated_date is not None else None,
                    "INFO:MA_TB": str(ma_tb),
                    "INFO:MA_GD": str(ma_gd),
                    "INFO:MA_KH": str(ma_kh),
                    "INFO:MA_HD": str(ma_hd),
                    "INFO:MA_HRM": str(ma_hrm),
                    "INFO:ADDRESS": str(address).encode('utf-8'),
                    "INFO:SOURCE": str(Source) if Source is not None else None,
                    "INFO:REQUEST_CERT_ID": str(request_cert_id),
                    "INFO:SERIAL": str(serial),
                    "INFO:CREDENTIAL_ID": str(credential_id),
                    "INFO:PRICING_CODE": str(PricingCode),
                    "INFO:PRICING_NAME": str(pricing_name).encode('utf-8'),
                    "INFO:PRICING_PRICE": str(Price),
                    "INFO:CODE": str(Code),
                    "INFO:SIGN_TYPE": str(SignType) if SignType is not None else None,
                    "INFO:VALIDITY": str(Validity) if Validity is not None else None,
                    "INFO:MA_GT": str(MaGt),
                    "INFO:PREVIOUS_SERIAL": str(PreviousSerial),
                    "INFO:CONTRACT_URL": str(contract_url).encode('utf-8'),
                    "INFO:ACCEPTANCE_URL": str(acceptance_url).encode('utf-8'),
                    "INFO:LOG_CONTENT": str(log_content).encode('utf-8'),
                    "INFO:LOG_CREATED_DATE": str(log_created_date) if log_created_date is not None else None,
                    "ADDRESS:PROVINCE_ID": str(provinceId) if provinceId is not None else None,
                    "ADDRESS:PROVINCE_NAME": str(provinceName).encode('utf-8'),
                    "ADDRESS:DISTRICT_ID": str(districtId) if districtId is not None else None,
                    "ADDRESS:DISTRICT_NAME": str(districtName).encode('utf-8'),
                    "ADDRESS:WARD_ID": str(wardId) if wardId is not None else None,
                    "ADDRESS:WARD_NAME": str(wardName).encode('utf-8'),
                    "ADDRESS:STREET_NAME": str(streetName).encode('utf-8'),
                    "ADDRESS:ADDRESS_DETAIL": str(address).encode('utf-8'),
                }
                batch.put(row_key, data)
            else:
                # Nếu bản ghi đã tồn tại, cập nhật một số trường thay đổi
                if _column_value_exists(table, row_key, "INFO", "STATUS", status):
                    table.put(row_key, {"INFO:STATUS": str(status) if status is not None else None})
                if _column_value_exists(table, row_key, "INFO", "STATUS_DESC", status_desc):
                    table.put(row_key, {"INFO:STATUS_DESC": str(status_desc)})
                if _column_value_exists(table, row_key, "INFO", "UPDATED_DATE", updated_date):
                    table.put(row_key, {"INFO:UPDATED_DATE": str(updated_date) if updated_date is not None else None})
                if _column_value_exists(table, row_key, "INFO", "REQUEST_CERT_ID", request_cert_id):
                    table.put(row_key, {"INFO:REQUEST_CERT_ID": str(request_cert_id)})
                if _column_value_exists(table, row_key, "INFO", "CREDENTIAL_ID", credential_id):
                    table.put(row_key, {"INFO:CREDENTIAL_ID": str(credential_id)})
                if _column_value_exists(table, row_key, "INFO", "CONTRACT_URL", contract_url):
                    table.put(row_key, {"INFO:CONTRACT_URL": str(contract_url).encode('utf-8')})
                if _column_value_exists(table, row_key, "INFO", "ACCEPTANCE_URL", acceptance_url):
                    table.put(row_key, {"INFO:ACCEPTANCE_URL": str(acceptance_url).encode('utf-8')})
                if _column_value_exists(table, row_key, "INFO", "LOG_CONTENT", log_content):
                    table.put(row_key, {"INFO:LOG_CONTENT": str(log_content).encode('utf-8')})
                if _column_value_exists(table, row_key, "INFO", "LOG_CREATED_DATE", log_created_date):
                    table.put(row_key,
                              {"INFO:LOG_CREATED_DATE": str(log_created_date) if log_created_date is not None else None})
                if _column_value_exists(table, row_key, "INFO", "SERIAL", serial):
                    table.put(row_key, {"INFO:SERIAL": str(serial)})

        batch.send()
        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        hbase_connection.close()


async def load_user(node_name, start_date, end_date):
    table = ["USER_INFO"]

    collection_user, mongo_client = init_connect_mongo(node_name, "User")
    try:
        documents_user = collection_user.find({'$and': [
            {'$or': [{'createDate': {'$gte': start_date}}, {'modifiedDate': {'gte': start_date}}]},
            {'createDate': {'$lt': end_date}}]},
            no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(table, "User", documents_user)
    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng User: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        mongo_client.close()
    logger.info("Chuyển dữ liệu User thành công từ MongoDB sang HBase.")

    collection_register, mongo_client = init_connect_mongo(node_name, "Register")
    try:
        documents_register = collection_register.find({'$and': [
            {'$or': [{'createdDate': {'$gte': start_date}}, {'modifiedDate': {'gte': start_date}}]},
            {'createdDate': {'$lt': end_date}}]},
            no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(table, "Register", documents_register)
    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng Register: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        mongo_client.close()
    logger.info("Chuyển dữ liệu Register thành công từ MongoDB sang HBase.")


async def load_personal_turn_order(node_name, start_date, end_date):
    collection, mongo_client = init_connect_mongo(node_name, "PersonalSignTurnOrder")
    table = ["PERSONAL_SIGN_TURN_ORDER"]
    try:
        documents = collection.find({'$and': [
            {'$or': [{'CreatedDate': {'$gte': start_date}}, {'UpdatedDate': {'gte': start_date}}]},
            {'CreatedDate': {'$lt': end_date}}]},
            no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(table, "PersonalSignTurnOrder", documents)
    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng PersonalSignTurnOrder: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        mongo_client.close()
    logger.info("Chuyển dữ liệu PersonalSignTurnOrder thành công từ MongoDB sang HBase.")


async def load_data_csc(start_date, end_date):
    try:
        node_name = "signservice_credential"
        await load_credential(node_name, start_date, end_date)
        await load_cert(node_name, start_date, end_date)
        await load_signature_transaction(node_name, start_date, end_date)
    except Exception as e:
        logger.error(f"Bỏ qua bảng do lỗi: {str(e)}")
        logger.error(traceback.format_exc())


async def load_credential(node_name, start_date, end_date):
    collection, mongo_client = init_connect_mongo(node_name, "Credential")
    table = ["CREDENTIAL", "CREDENTIAL_STATUS_LOG"]
    try:
        documents = collection.find({'$and': [
            {'$or': [{'createdDate': {'$gte': start_date}}, {'modifiedDate': {'gte': start_date}}]},
            {'createdDate': {'$lt': end_date}}]},
            no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(table, "Credential", documents)

    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng Credential: {str(e)} ")
        logger.error(traceback.format_exc())
    finally:
        mongo_client.close()
    logger.info("Chuyển dữ liệu Credential thành công từ MongoDB sang HBase.")


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

    try:
        collection_cert, mongo_client = init_connect_mongo(node_name, "Cert")
        documents_cert = collection_cert.find({'$and': [
            {'createDate': {'$gte': start_date}},
            {'createDate': {'$lt': end_date}}]},
            no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(table, "Cert", documents_cert)
    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng Cert: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        mongo_client.close()
    logger.info("Chuyển dữ liệu Cert thành công từ MongoDB sang HBase.")


async def _get_cut_off_name(node_name, table_name, start_date, end_date):
    all_data = [table_name]
    collection, mongo_client = init_connect_mongo(node_name, "CutOff")
    try:
        if start_date is None:
            if end_date is None:
                documents = collection.find({'sourceCollection': table_name})
            else:
                documents = collection.find({'$and': [
                    {'fromTime': {'$lte': end_date}},
                    {'sourceCollection': table_name}]})
        elif end_date is None:
            documents = collection.find({'$and': [
                {'toTime': {'$lt': end_date}},
                {'sourceCollection': table_name}]})
        else:
            documents = collection.find({'$and': [
                {'toTime': {'$gte': start_date}},
                {'fromTime': {'$lte': end_date}},
                {'sourceCollection': table_name}]})

        for document in documents:
            collectionName = str(document.get("collectionName", ""))
            all_data.append(collectionName)
        logger.info(f"Get table name cut off table {table_name} from MongoDB.")
    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng SignatureTransaction: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        mongo_client.close()
        return all_data


async def load_signature_transaction(node_name, start_date, end_date):
    try:
        table_name = "SignatureTransaction"
        table_names = await _get_cut_off_name(node_name, table_name, start_date, end_date)
        for name in table_names:
            collection, mongo_client = init_connect_mongo(node_name, name)
            table = ["SIGNATURE_TRANSACTION"]
            documents = collection.find({'$and': [
                {'reqTime': {'$gte': start_date}},
                {'reqTime': {'$lt': end_date}}]},
                no_cursor_timeout=True).batch_size(CHUNK_SIZE)

            # Xử lý song song bất đồng bộ
            await _process_chunks(table, "SignatureTransaction", documents)
            mongo_client.close()

    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng SignatureTransaction: {str(e)}")
        logger.error(traceback.format_exc())


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
        match collection_name:
            case "User":
                await _transfer_user(chunk, table)
            case "Register":
                await _transfer_register(chunk, table)
            case "CertOrder":
                await _transfer_cert_order(chunk, table)
            case "PersonalSignTurnOrder":
                await _transfer_personal_sign_turn_order(chunk, table)
            case "Cert":
                await _transfer_cert(chunk, table)
            case "RequestCert":
                await _transfer_request_cert(chunk, table)
            case "Credential":
                await _transfer_credential(chunk, table)
            case "SignatureTransaction":
                await _transfer_signature_transaction(chunk, table)
            case _:
                logger.info(f"Bảng {collection_name} không được nhận diện.")
                return None
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý chunk: {str(e)}")
        logger.error(traceback.format_exc())


async def _transfer_signature_transaction(chunk, table_names):
    try:
        tables, hbase_connection = init_connect_hbase(table_names)
        table = tables[0]
        batch = table.batch()
        for document in chunk:
            row_key = str(document["_id"])
            credentialId = str(document.get("credentialId", ""))
            serial = str(document.get("certSerial", ""))
            identityId = str(document.get("identityId", ""))
            uid = str(document.get("identityUid", ""))
            fullName = str(document.get("identityName", ""))
            email = str(document.get("identityEmail", ""))
            status = str(document.get("status", ""))
            statusDesc = str(document.get("statusDesc", ""))
            reqTime = _convert_datetime(document.get("reqTime", ""))
            expiredTime = _convert_datetime(document.get("expireTime", ""))
            finishDate = _convert_datetime(document.get("finishDate", ""))
            tranTypeDesc = str(document.get("tranTypeDesc", ""))
            tranType = _convert_number(document.get("tranType", ""))
            executeTime = str(document.get("excuteTime", ""))
            appId = str(document.get("appId", ""))
            appName = str(document.get("appName", ""))
            tranCode = str(document.get("tranCode", ""))
            data = {
                "INFO:SERIAL": serial,
                "INFO:CREDENTIAL_ID": credentialId,
                "INFO:IDENTITY_ID": identityId,
                "INFO:UID": uid,
                "INFO:FULL_NAME": fullName.encode('utf-8'),
                "INFO:EMAIL": email,
                "INFO:STATUS": status,
                "INFO:STATUS_DESC": statusDesc,
                "INFO:REQ_TIME": reqTime,
                "INFO:EXPIRED_TIME": expiredTime,
                "INFO:FINISH_DATE": finishDate,
                "INFO:TRAN_TYPE_DESC": tranTypeDesc,
                "INFO:TRAN_TYPE": tranType,
                "INFO:EXECUTE_TIME": executeTime,
                "INFO:APP_ID": appId,
                "INFO:APP_NAME": appName,
                "INFO:TRAN_CODE": tranCode,
            }
            batch.put(row_key, data)
        batch.send()
        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        hbase_connection.close()


async def _transfer_register(chunk, table_names):
    try:
        tables, hbase_connection = init_connect_hbase(table_names)
        table = tables[0]
        batch = table.batch()
        cf_prefix = b'REGISTER:'
        for document in chunk:
            row_key = str(document["_id"])
            status = _convert_number(document.get("status", ""))
            statusDesc = document.get("statusDesc", "")
            modifiedDate = _convert_datetime(document.get("modifiedDate", ""))
            if document.get('address'):
                identityAddress = document.get('address', {})
                provinceId = _convert_number(identityAddress.get("provinceId", ""))
                districtId = _convert_number(identityAddress.get("districtId", ""))
                wardId = _convert_number(identityAddress.get("wardId", ""))
                streetName = identityAddress.get("streetName", "")
                address = identityAddress.get("address", "")
            else:
                provinceId = districtId = wardId = streetName = address = ""

            put_data = {}

            old_item = table.row(row_key)
            if old_item is None or old_item == {} or any(key.startswith(cf_prefix) for key in old_item.keys()) is False:
                fullName = document.get("fullName", "")
                email = document.get("email", "")
                phone = document.get("phone", "")
                uid = document.get("uid", "")
                createDate = _convert_datetime(document.get("createdDate", ""))
                source = document.get("source", "")

                put_data.update({
                    "REGISTER:FULL_NAME": str(fullName).encode('utf-8'),
                    "REGISTER:UID": str(uid),
                    "REGISTER:PHONE": str(phone),
                    "REGISTER:EMAIL": str(email),
                    "REGISTER:STATUS": str(status),
                    "REGISTER:STATUS_DESC": str(statusDesc),
                    "REGISTER:CREATE_DATE": str(createDate),
                    "REGISTER:MODIFIED_DATE": str(modifiedDate),
                    "REGISTER:SOURCE": str(source),
                    "REGISTER:PROVINCE_ID": str(provinceId),
                    "REGISTER:DISTRICT_ID": str(districtId),
                    "REGISTER:WARD_ID": str(wardId),
                    "REGISTER:STREET_NAME": str(streetName).encode('utf-8'),
                    "REGISTER:ADDRESS": str(address)
                })
            else:
                if _column_value_exists(table, row_key, "REGISTER", "STATUS", status):
                    put_data["REGISTER:STATUS"] = str(status)
                if _column_value_exists(table, row_key, "REGISTER", "STATUS_DESC", statusDesc):
                    put_data["REGISTER:STATUS_DESC"] = str(statusDesc)
                if _column_value_exists(table, row_key, "REGISTER", "MODIFIED_DATE", modifiedDate):
                    put_data["REGISTER:MODIFIED_DATE"] = str(modifiedDate)

            if put_data:
                batch.put(row_key, put_data)

        batch.send()
        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        hbase_connection.close()


async def _transfer_user(chunk, table_names):
    try:
        tables, hbase_connection = init_connect_hbase(table_names)
        table = tables[0]
        batch = table.batch()
        cf_prefix = b"INFO:"
        for document in chunk:
            row_key = str(document["_id"])
            status = _convert_number(document.get("status", ""))
            statusDesc = document.get("statusDesc", "")
            modifiedDate = _convert_datetime(document.get("modifiedDate", ""))
            provinceCode = document.get("provinceCode", "")
            provinceCodes = document.get("provinceCodes", "")
            provinceCode_str = ", ".join(doc.get('name', '') for doc in provinceCodes if isinstance(doc, dict))
            role = document.get("roles", [])
            roles = ", ".join(doc.get('name', '') for doc in role if isinstance(doc, dict))
            preStatus = document.get("preStatus", "")

            old_item = table.row(row_key)
            data_to_put = {}

            if old_item is None or old_item == {} or any(key.startswith(cf_prefix) for key in old_item.keys()) is False:
                data_to_put.update({
                    "INFO:UID": str(document.get("uid", "")),
                    "INFO:USERNAME": str(document.get("username", "")),
                    "INFO:FULL_NAME": _safe_encode(document.get("fullName", "")),
                    "INFO:PHONE": str(document.get("phone", "")),
                    "INFO:EMAIL": str(document.get("email", "")),
                    "INFO:LOCALITY_CODE": str(document.get("localityId", "")),
                    "INFO:UID_PREFIX": str(document.get("uidPrefix", "")),
                    "INFO:USER_GROUP_ID": str(document.get("userGroupId", "")),
                    "INFO:STATUS": str(status),
                    "INFO:STATUS_DESC": str(statusDesc),
                    "INFO:CREATE_DATE": str(_convert_datetime(document.get("createDate", ""))),
                    "INFO:MODIFIED_DATE": str(modifiedDate),
                    "INFO:ACCOUNT_TYPE": str(_convert_number(document.get("accountType", ""))),
                    "INFO:ACCOUNT_TYPE_DESC": str(document.get("accountTypeDesc", "")),
                    "INFO:IS_COM_ADMIN": str(_convert_boolean(document.get("isComAdmin", ""))),
                    "INFO:IS_TEST": str(_convert_boolean(document.get("isTest", ""))),
                    "INFO:PRE_STATUS": str(preStatus),
                    "INFO:PROVINCE_CODE": str(provinceCode),
                    "INFO:PROVINCE_CODES": str(provinceCode_str),
                    "INFO:SOURCE": str(_convert_number(document.get("source", ""))),
                    "INFO:ROLES": str(roles),
                })

                if document.get('identityAddress'):
                    identity = document['identityAddress']
                    data_to_put.update({
                        "INFO:PROVINCE_ID": str(_convert_number(identity.get("provinceId", ""))),
                        "INFO:PROVINCE_NAME": _safe_encode(identity.get("provinceName", "")),
                        "INFO:DISTRICT_ID": str(_convert_number(identity.get("districtId", ""))),
                        "INFO:DISTRICT_NAME": _safe_encode(identity.get("districtName", "")),
                        "INFO:WARD_ID": str(_convert_number(identity.get("wardId", ""))),
                        "INFO:WARD_NAME": _safe_encode(identity.get("wardName", "")),
                        "INFO:STREET_NAME": _safe_encode(identity.get("streetName", "")),
                        "INFO:ADDRESS": _safe_encode(identity.get("address", ""))
                    })

            else:
                if _column_value_exists(table, row_key, "INFO", "STATUS", status):
                    data_to_put["INFO:STATUS"] = str(status)
                if _column_value_exists(table, row_key, "INFO", "STATUS_DESC", statusDesc):
                    data_to_put["INFO:STATUS_DESC"] = str(statusDesc)
                if _column_value_exists(table, row_key, "INFO", "MODIFIED_DATE", modifiedDate):
                    data_to_put["INFO:MODIFIED_DATE"] = str(modifiedDate)
                if _column_value_exists(table, row_key, "INFO", "PROVINCE_CODE", provinceCode):
                    data_to_put["INFO:PROVINCE_CODE"] = str(provinceCode)
                if _column_value_exists(table, row_key, "INFO", "PROVINCE_CODES", provinceCodes):
                    data_to_put["INFO:PROVINCE_CODES"] = str(provinceCode_str)
                if _column_value_exists(table, row_key, "INFO", "ROLES", roles):
                    data_to_put["INFO:ROLES"] = str(roles)
                if _column_value_exists(table, row_key, "INFO", "PRE_STATUS", preStatus):
                    data_to_put["INFO:PRE_STATUS"] = str(preStatus)

            if data_to_put:
                batch.put(row_key, data_to_put)

        batch.send()
        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        hbase_connection.close()


async def _transfer_personal_sign_turn_order(chunk, table_names):
    try:
        tables, hbase_connection = init_connect_hbase(table_names)
        table = tables[0]
        batch = table.batch()
        for document in chunk:
            row_key = str(document["_id"])

            Status = _convert_number(document.get("Status", ""))
            StatusDesc = document.get("StatusDesc", "")
            UpdatedDate = _convert_datetime(document.get("UpdatedDate", ""))
            PaymentStatus = _convert_number(document.get("PaymentStatus", ""))
            PaymentStatusDesc = document.get("PaymentStatuDesc", "")
            TotalMoney = document.get("TotalMoney", "")
            Pricings = document.get('Pricings', [])
            PricingName, PricingCode, Code, SignTurnNumber = "", "", "", 0
            if Pricings:
                Pricing = Pricings[-1]
                PricingName = Pricing.get("Name", "")
                PricingCode = Pricing.get("tocdo_id", "")
                Code = Pricing.get("Code", "")
                SignTurnNumber = _convert_number(Pricing.get("SignTurnNumber", 0))

            # Trích xuất thông tin UserInfo
            Identity = document.get('UserInfo', {})
            IdentityId = Identity.get('_id', '')
            Uid = Identity.get("Uid", "")
            FullName = Identity.get("FullName", "")
            LocalityCode = Identity.get("LocalityCode", "")

            # Trích xuất các thông tin khác
            CreatedDate = _convert_datetime(document.get("CreatedDate", ""))
            DHSXKDCustomerInfo = document.get("DHSXKDCustomerInfo", {})
            ma_tb = DHSXKDCustomerInfo.get("ma_tb", "")
            ma_gd = DHSXKDCustomerInfo.get("ma_gd", "")
            ma_kh = DHSXKDCustomerInfo.get("ma_kh", "")
            ma_hd = DHSXKDCustomerInfo.get("ma_hd", "")
            ma_hrm = DHSXKDCustomerInfo.get("ma_hrm", "")
            CredentialId = document.get("CredentialId", "")
            PaymentOrderId = document.get("PaymentOrderId", "")
            IsSyncDHSXKD = _convert_boolean(document.get("IsSyncDHSXKD", ""))
            MaGt = document.get("MaGt", "")

            # Ghi vào HBase
            table.put(row_key, {
                "INFO:IDENTITY_ID": str(IdentityId),
                "INFO:UID": str(Uid),
                "INFO:FULL_NAME": str(FullName).encode('utf-8'),
                "INFO:LOCALITY_CODE": str(LocalityCode),
                "INFO:STATUS": str(Status) if Status else b"",
                "INFO:STATUS_DESC": str(StatusDesc),
                "INFO:CREATED_DATE": str(CreatedDate) if CreatedDate else b"",
                "INFO:UPDATED_DATE": str(UpdatedDate) if UpdatedDate else b"",
                "INFO:MA_TB": str(ma_tb),
                "INFO:MA_GD": str(ma_gd),
                "INFO:MA_KH": str(ma_kh),
                "INFO:MA_HD": str(ma_hd),
                "INFO:MA_HRM": str(ma_hrm),
                "INFO:CREDENTIAL_ID": str(CredentialId),
                "INFO:PAYMENT_ORDER_ID": str(PaymentOrderId),
                "INFO:PAYMENT_STATUS": str(PaymentStatus) if PaymentStatus else b"",
                "INFO:PAYMENT_STATUS_DESC": str(PaymentStatusDesc),
                "INFO:IS_SYNC_DHSXKD": str(IsSyncDHSXKD),
                "INFO:MA_GT": str(MaGt),
                "INFO:PRICING_NAME": str(PricingName).encode('utf-8'),
                "INFO:PRICING_CODE": str(PricingCode),
                "INFO:CODE": str(Code),
                "INFO:SIGN_TURN_NUMBER": str(SignTurnNumber),
                "INFO:TOTAL_MONEY": str(TotalMoney) if TotalMoney else b""
            })

            # Nếu đã có bản ghi trong HBase, kiểm tra và cập nhật các trường thay đổi
            old_item = table.row(row_key)
            if old_item:
                if _column_value_exists(table, row_key, "INFO", "STATUS", Status):
                    table.put(row_key, {'INFO:STATUS': str(Status)})
                if _column_value_exists(table, row_key, "INFO", "STATUS_DESC", StatusDesc):
                    table.put(row_key, {'INFO:STATUS_DESC': str(StatusDesc)})
                if _column_value_exists(table, row_key, "INFO", "UPDATED_DATE", UpdatedDate):
                    table.put(row_key, {'INFO:UPDATED_DATE': str(UpdatedDate)})
                if _column_value_exists(table, row_key, "INFO", "PAYMENT_STATUS", PaymentStatus):
                    table.put(row_key, {'INFO:PAYMENT_STATUS': str(PaymentStatus)})
                if _column_value_exists(table, row_key, "INFO", "PAYMENT_STATUS_DESC", PaymentStatusDesc):
                    table.put(row_key, {'INFO:PAYMENT_STATUS_DESC': str(PaymentStatusDesc)})
                if _column_value_exists(table, row_key, "INFO", "PRICING_NAME", PricingName):
                    table.put(row_key, {'INFO:PRICING_NAME': str(PricingName).encode('utf-8')})
                if _column_value_exists(table, row_key, "INFO", "PRICING_CODE", PricingCode):
                    table.put(row_key, {'INFO:PRICING_CODE': str(PricingCode)})
                if _column_value_exists(table, row_key, "INFO", "CODE", Code):
                    table.put(row_key, {'INFO:CODE': str(Code)})
                if _column_value_exists(table, row_key, "INFO", "SIGN_TURN_NUMBER", SignTurnNumber):
                    table.put(row_key, {'INFO:SIGN_TURN_NUMBER': str(SignTurnNumber)})
                if _column_value_exists(table, row_key, "INFO", "TOTAL_MONEY", TotalMoney):
                    table.put(row_key, {'INFO:TOTAL_MONEY': str(TotalMoney)})

        batch.send()
        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        hbase_connection.close()


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


async def _transfer_cert(chunk, table_names):
    try:
        tables, hbase_connection = init_connect_hbase(table_names)
        table = tables[0]
        batch = table.batch()
        for document in chunk:
            row_key = str(document["requestId"])
            serial = str(document.get("serial", ""))
            status = str(document.get("status", ""))
            statusDesc = str(document.get("statusDesc", ""))
            subject = str(document.get("subject", ""))
            validFrom = _convert_datetime(document.get("validFrom", ""))
            validTo = _convert_datetime(document.get("validTo", ""))
            createdDate = _convert_datetime(document.get("createDate", ""))
            # Ghi vào HBase
            table.put(row_key, {
                "INFO:CERT_STATUS": status,
                "INFO:CERT_STATUS_DESC": statusDesc,
                "INFO:SERIAL": serial,
                "INFO:SUBJECT": subject,
                "INFO:VALID_FROM": validFrom,
                "INFO:VALID_TO": validTo,
                "INFO:CERT_CREATED_DATE": createdDate,
            })
        batch.send()
        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        hbase_connection.close()


async def _transfer_credential(chunk, table_names):
    try:
        tables, hbase_connection = init_connect_hbase(table_names)
        table_credential = tables[0]
        batch_credential = table_credential.batch()
        table_log = tables[1]
        batch_log = table_log.batch()
        keyword = "OPER"
        pricing_ps0 = {
            "Chứng thư số cá nhân trên các ứng dụng",
            "SmartCA cá nhân PS0 (Công dân)",
            "SmartCA Personal App Sign Limit Std"
        }
        for document in chunk:
            row_key = str(document["_id"])
            fullName = document.get("identity", {}).get("name", "")
            if fullName is not None and "test" in fullName.lower():
                continue

            pricingName = document.get("contract", {}).get("servicePack", "")
            if pricingName is not None and "test" in pricingName.lower():
                continue
            subjectDN = document.get("subjectDN", "")
            pattern_1 = "CN=[^,]+ Test,"
            pattern_2 = "CN=[^,]+ TEST,"
            pattern_3 = "CN=[^,]+ test,"
            if subjectDN is not None and (
                    re.search(pattern_1, subjectDN) or re.search(pattern_2, subjectDN) or re.search(pattern_3,
                                                                                                    subjectDN)):
                continue

            status = _convert_number(document.get("status", ""))
            statusDesc = document.get("statusDesc", "")
            modifiedDate = _convert_datetime(document.get("modifiedDate", ""))
            row_log_key = row_key + modifiedDate if modifiedDate else ""
            certs = document.get('certs', [])
            if certs is not None and certs != [] and certs:
                cert = certs[-1]
                certStatus = _convert_number(cert.get("status", ""))
                certStatusDesc = cert.get("statusDesc", "")
                validFrom = _convert_datetime(cert.get("validFrom", ""))
                validTo = _convert_datetime(cert.get("validTo", ""))
                requestCertId = cert.get("requestId", "")
                serial = cert.get("serial", "")
            else:
                validFrom = ""
                validTo = ""
                requestCertId = ""
                serial = ""
                certStatus = ""
                certStatusDesc = ""

            # Kiểm tra record đã tồn tại trong bảng credential hay chưa
            old_item = table_credential.row(row_key)
            if old_item is None or old_item == {}:
                createdDate = _convert_datetime(document.get("createdDate", ""))
                identity = document.get("identity", {})
                identityId = identity.get("_id", "")
                email = identity.get("email", "")
                phone = identity.get("phone", "")
                username = identity.get("username", "")
                uid = identity.get("uid", "")
                clientId = identity.get("createdByClientId", "")
                clientName = identity.get("createdByClientName", "")
                localityCode = identity.get("LocalityCode", "")
                if (localityCode is None or str.isspace(localityCode)) and username is not None and username != "":
                    index = username.find(keyword)
                    if index != -1:
                        localityCode = username[index:index + 3]
                    else:
                        localityCode = username[3:6]
                ma_tb = identity.get("DHSXKDSubcriptionCode", "")
                source = identity.get("source", "")
                contractNumber = document.get("contract", {}).get("number", "")

                validity = _convert_number(document.get("contract", "").get("validity", ""))
                pricingCode = document.get("contract", "").get("pricingCode", "")
                if (pricingCode is None or str.isspace(pricingCode)) and pricingName in pricing_ps0:
                    pricingCode = 17187
                serviceType = document.get("contract", "").get("serviceType", "")

                # Ghi vào HBase
                table_credential.put(row_key, {
                    "INFO:SUBJECT_DN": str(subjectDN).encode('utf-8'),
                    "INFO:STATUS": str(status),
                    "INFO:STATUS_DESC": str(statusDesc),
                    "INFO:CREATED_DATE": str(createdDate),
                    "INFO:MODIFIED_DATE": str(modifiedDate),
                    "INFO:IDENTITY_ID": str(identityId),
                    "INFO:FULL_NAME": str(fullName).encode('utf-8'),
                    "INFO:USERNAME": str(username),
                    "INFO:EMAIL": str(email),
                    "INFO:PHONE": str(phone),
                    "INFO:UID": str(uid),
                    "INFO:CLIENT_ID": str(clientId),
                    "INFO:CLIENT_NAME": str(clientName).encode('utf-8'),
                    "INFO:LOCALITY_CODE": str(localityCode),
                    "INFO:MA_TB": str(ma_tb),
                    "INFO:SOURCE": str(source),

                    "INFO:CONTRACT_NUMBER": str(contractNumber),
                    "INFO:PRICING_NAME": str(pricingName).encode('utf-8'),
                    "INFO:VALIDITY": str(validity),
                    "INFO:PRICING_CODE": str(pricingCode),
                    "INFO:SERVICE_TYPE": str(serviceType),

                    "INFO:SERIAL": str(serial),
                    "INFO:VALID_FROM": str(validFrom),
                    "INFO:VALID_TO": str(validTo),
                    "INFO:REQUEST_CERT_ID": str(requestCertId),
                    "INFO:CERT_STATUS": str(certStatus),
                    "INFO:CERT_STATUS_DESC": str(certStatusDesc)
                })

                table_log.put(str(row_log_key), {
                    "INFO:CREDENTIAL_ID": row_key,
                    "INFO:SERIAL": str(serial),
                    "INFO:VALID_FROM": str(validFrom),
                    "INFO:VALID_TO": str(validTo),
                    "INFO:STATUS": str(status),
                    "INFO:REQUEST_CERT_ID": str(requestCertId),
                    "INFO:STATUS_DESCRIPTION": str(statusDesc),
                    "INFO:MODIFIED_DATE": str(modifiedDate)
                })
            else:
                if _column_value_exists(table_credential, row_key, "INFO", "STATUS", status):
                    table_credential.put(row_key, {'INFO:STATUS': str(status)})
                    table_log.put(str(row_log_key), {
                        "INFO:CREDENTIAL_ID": row_key,
                        "INFO:SERIAL": str(serial),
                        "INFO:VALID_FROM": str(validFrom),
                        "INFO:VALID_TO": str(validTo),
                        "INFO:STATUS": str(status),
                        "INFO:REQUEST_CERT_ID": str(requestCertId),
                        "INFO:STATUS_DESCRIPTION": str(statusDesc),
                        "INFO:MODIFIED_DATE": str(modifiedDate)
                    })
                if _column_value_exists(table_credential, row_key, "INFO", "SUBJECT_DN", subjectDN):
                    table_credential.put(row_key, {'INFO:SUBJECT_DN': str(subjectDN).encode('utf-8')})
                if _column_value_exists(table_credential, row_key, "INFO", "STATUS_DESC", statusDesc):
                    table_credential.put(row_key, {'INFO:STATUS_DESC': str(statusDesc)})
                if _column_value_exists(table_credential, row_key, "INFO", "MODIFIED_DATE", modifiedDate):
                    table_credential.put(row_key, {'INFO:MODIFIED_DATE': str(modifiedDate)})
                if _column_value_exists(table_credential, row_key, "INFO", "SERIAL", serial):
                    table_credential.put(row_key, {'INFO:SERIAL': str(serial)})
                if _column_value_exists(table_credential, row_key, "INFO", "VALID_FROM", validFrom):
                    table_credential.put(row_key, {'INFO:VALID_FROM': str(validFrom)})
                if _column_value_exists(table_credential, row_key, "INFO", "VALID_TO", validTo):
                    table_credential.put(row_key, {'INFO:VALID_TO': str(validTo)})
                if _column_value_exists(table_credential, row_key, "INFO", "REQUEST_CERT_ID", requestCertId):
                    table_credential.put(row_key, {'INFO:REQUEST_CERT_ID': str(requestCertId)})
                if _column_value_exists(table_credential, row_key, "INFO", "CERT_STATUS", certStatus):
                    table_credential.put(row_key, {'INFO:CERT_STATUS': str(certStatus)})
                if _column_value_exists(table_credential, row_key, "INFO", "CERT_STATUS_DESC", certStatusDesc):
                    table_credential.put(row_key, {'INFO:CERT_STATUS_DESC': str(certStatusDesc)})

        batch_credential.send()
        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")

        batch_log.send()
        logger.info(f"Batch với {len(chunk)} bản ghi đã được lưu log.")
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


def _safe_encode(value):
    return value.encode('utf-8') if isinstance(value, str) else value
