import asyncio
import re
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
    await load_data_csc(start_date, end_date)


async def load_data_identity(start_date, end_date):
    try:
        node_name = "signservice_identity"
        await load_user(node_name, start_date, end_date)
        await load_cert_order(node_name, start_date, end_date)
        await load_personal_turn_order(node_name, start_date, end_date)
    except Exception as e:
        logger.error(f"Bỏ qua bảng do lỗi: {e}")
        traceback.print_exc()


async def load_cert_order(node_name, start_date, end_date):
    collection, table, hbase_connection, mongo_client = init_connect(config, node_name, "CertOrder", ["CERT_ORDER"])
    try:
        documents = collection.find({'$and': [
            {'$or': [{'CreatedDate': {'$gte': start_date}}, {'UpdatedDate': {'gte': start_date}}]},
            {'CreatedDate': {'$lt': end_date}}]},
            no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(table, "CertOrder", documents)
    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng CertOrder: {e} ")
        traceback.print_exc()
    finally:
        hbase_connection.close()
        mongo_client.close()
    logger.info("Chuyển dữ liệu CertOrder thành công từ MongoDB sang HBase.")


async def _transfer_cert_order(chunk, tables):
    table = tables[0]
    batch = table.batch()
    try:
        for document in chunk:
            FullName = document.get("FullName", "")
            if FullName and "test" in FullName.lower():
                continue
            Pricing = document.get("Pricing", {})
            if Pricing is None or Pricing is {}:
                continue
            PricingName = Pricing.get("PricingName", "")
            if PricingName and "test" in PricingName.lower():
                continue
            DHSXKDCustomerInfo = document.get("DHSXKDCustomerInfo", {})
            if DHSXKDCustomerInfo is None or DHSXKDCustomerInfo is {}:
                continue
            ma_gd = DHSXKDCustomerInfo.get("ma_gd", "")
            if ma_gd and "test" in ma_gd.lower():
                continue
            row_key = str(document["_id"])
            Status = document.get("Status", "")
            StatusDesc = document.get("StatusDesc", "")
            LogContent = None
            LogCreatedDate = None
            UpdatedDate = ""
            UpdatedDate_check = document.get("UpdatedDate", "")
            if UpdatedDate_check is not None and UpdatedDate_check != "" and UpdatedDate_check != datetime(1, 1, 1, 0, 0):
                UpdatedDate = int(UpdatedDate_check.timestamp() * 1000)
                if Status is not None and 50 <= int(Status) < 99:
                    Logs = document.get("Logs", [])
                    if Logs is not None and Logs is not {} and Logs:
                        for record in reversed(Logs):
                            if record.get("IsError", "") == "True" or record.get("IsError", "") == "true" or record.get("IsError", "") == "1":
                                LogContent = record.get("Content", "")
                                LogCreatedDate = int(record.get("CreatedDate", "").timestamp() * 1000)
                                break

            CredentialId = document.get("CredentialId", "")
            RequestCertId = document.get("RequestCertId", "")
            ContractUrl = document.get("ContractUrl", "")
            Serial = document.get("Serial", "")
            AcceptanceUrl = ""
            Acceptance = document.get('AcceptanceDocuments', [])
            if Acceptance is not None and Acceptance is not [] and Acceptance:
                AcceptanceUrl = Acceptance[-1].get('UrlSigned', '') if document.get(
                    'AcceptanceDocuments') else ''

            old_item = table.row(row_key)
            if old_item is None or old_item == {}:
                IdentityId = str(document.get("IdentityId", ""))
                Uid = document.get("Uid", "")

                Email = document.get("Email", "")
                Phone = document.get("Phone", "")
                LocalityCode = document.get("LocalityCode", "")
                Type = document.get("Type", "")
                TypeDesc = document.get("TypeDesc", "")

                ClientName = document.get("ClientName", "")
                ClientId = document.get("ClientId", "")
                CreatedDate = int(document.get("CreatedDate", "").timestamp() * 1000)

                ma_tb = DHSXKDCustomerInfo.get("ma_tb", "")

                ma_kh = DHSXKDCustomerInfo.get("ma_kh", "")
                ma_hd = DHSXKDCustomerInfo.get("ma_hd", "")
                ma_hrm = DHSXKDCustomerInfo.get("ma_hrm", "")
                #address
                Source = document.get("Source", "")
                #serial

                PricingCode = Pricing.get("PricingCode", "")

                Price = Pricing.get("Price", "")
                Code = Pricing.get("Code", "")
                SignType = Pricing.get("SignType", "")
                Validity = Pricing.get("Validity", "")
                MaGt = document.get("MaGt", "")

                PreviousSerial = document.get("PreviousSerial", "")

                if document.get('Address'):
                    identity_address = document.get('Address', {})
                    provinceId = identity_address.get("provinceId", "")
                    provinceName = identity_address.get("provinceName", "")
                    districtId = identity_address.get("districtId", "")
                    districtName = identity_address.get("districtName", "")
                    wardId = identity_address.get("wardId", "")
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
                    "INFO:IDENTITY_ID": IdentityId,
                    "INFO:UID": str(Uid),
                    "INFO:FULL_NAME": str(FullName).encode('utf-8'),
                    "INFO:EMAIL": str(Email),
                    "INFO:PHONE": str(Phone),
                    "INFO:LOCALITY_CODE": str(LocalityCode),
                    "INFO:TYPE": str(Type) if Type is not None else None,
                    "INFO:TYPE_DESC": str(TypeDesc),
                    "INFO:STATUS": str(Status) if Status is not None else None,
                    "INFO:STATUS_DESC": str(StatusDesc),
                    "INFO:CLIENT_NAME": str(ClientName).encode('utf-8'),
                    "INFO:CLIENT_ID": str(ClientId),
                    "INFO:CREATED_DATE": str(CreatedDate),
                    "INFO:UPDATED_DATE": str(UpdatedDate) if UpdatedDate is not None else None,
                    "INFO:MA_TB": str(ma_tb),
                    "INFO:MA_GD": str(ma_gd),
                    "INFO:MA_KH": str(ma_kh),
                    "INFO:MA_HD": str(ma_hd),
                    "INFO:MA_HRM": str(ma_hrm),
                    "INFO:ADDRESS": str(address).encode('utf-8'),
                    "INFO:SOURCE": str(Source) if Source is not None else None,
                    "INFO:REQUEST_CERT_ID": str(RequestCertId),
                    "INFO:SERIAL": str(Serial),
                    "INFO:CREDENTIAL_ID": str(CredentialId),
                    "INFO:PRICING_CODE": str(PricingCode),
                    "INFO:PRICING_NAME": str(PricingName).encode('utf-8'),
                    "INFO:PRICING_PRICE": str(Price),
                    "INFO:CODE": str(Code),
                    "INFO:SIGN_TYPE": str(SignType) if SignType is not None else None,
                    "INFO:VALIDITY": str(Validity) if Validity is not None else None,
                    "INFO:MA_GT": str(MaGt),
                    "INFO:PREVIOUS_SERIAL": str(PreviousSerial),
                    "INFO:CONTRACT_URL": str(ContractUrl).encode('utf-8'),
                    "INFO:ACCEPTANCE_URL": str(AcceptanceUrl).encode('utf-8'),
                    "INFO:LOG_CONTENT": str(LogContent).encode('utf-8'),
                    "INFO:LOG_CREATED_DATE": str(LogCreatedDate) if LogCreatedDate is not None else None,
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
                if _column_value_exists(table, row_key, "INFO", "STATUS", Status):
                    table.put(row_key, {"INFO:STATUS": str(Status) if Status is not None else None})
                if _column_value_exists(table, row_key, "INFO", "STATUS_DESC", StatusDesc):
                    table.put(row_key, {"INFO:STATUS_DESC": str(StatusDesc)})
                if _column_value_exists(table, row_key, "INFO", "UPDATED_DATE", UpdatedDate):
                    table.put(row_key, {"INFO:UPDATED_DATE": str(UpdatedDate) if UpdatedDate is not None else None})
                if _column_value_exists(table, row_key, "INFO", "REQUEST_CERT_ID", RequestCertId):
                    table.put(row_key, {"INFO:REQUEST_CERT_ID": str(RequestCertId)})
                if _column_value_exists(table, row_key, "INFO", "CREDENTIAL_ID", CredentialId):
                    table.put(row_key, {"INFO:CREDENTIAL_ID": str(CredentialId)})
                if _column_value_exists(table, row_key, "INFO", "CONTRACT_URL", ContractUrl):
                    table.put(row_key, {"INFO:CONTRACT_URL": str(ContractUrl).encode('utf-8')})
                if _column_value_exists(table, row_key, "INFO", "ACCEPTANCE_URL", AcceptanceUrl):
                    table.put(row_key, {"INFO:ACCEPTANCE_URL": str(AcceptanceUrl).encode('utf-8')})
                if _column_value_exists(table, row_key, "INFO", "LOG_CONTENT", LogContent):
                    table.put(row_key, {"INFO:LOG_CONTENT": str(LogContent).encode('utf-8')})
                if _column_value_exists(table, row_key, "INFO", "LOG_CREATED_DATE", LogCreatedDate):
                    table.put(row_key, {"INFO:LOG_CREATED_DATE": str(LogCreatedDate) if LogCreatedDate is not None else None})
                if _column_value_exists(table, row_key, "INFO", "SERIAL", Serial):
                    table.put(row_key, {"INFO:SERIAL": str(Serial)})

        batch.send()
        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {e}")
        traceback.print_exc()


async def load_user(node_name, start_date, end_date):
    collection_register, table, hbase_connection, mongo_client = init_connect(config, node_name, "Register", ["USER_INFO"])
    try:
        documents_register = collection_register.find({'$and': [
            {'$or': [{'createdDate': {'$gte': start_date}}, {'modifiedDate': {'gte': start_date}}]},
            {'createdDate': {'$lt': end_date}}]},
            no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(table, "Register", documents_register)
    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng Register: {e}")
        traceback.print_exc()
    finally:
        hbase_connection.close()
        mongo_client.close()
    logger.info("Chuyển dữ liệu Register thành công từ MongoDB sang HBase.")

    collection_user, table, hbase_connection, mongo_client = init_connect(config, node_name, "User", ["USER_INFO"])
    try:
        documents_user = collection_user.find({'$and': [
            {'$or': [{'createDate': {'$gte': start_date}}, {'modifiedDate': {'gte': start_date}}]},
            {'createDate': {'$lt': end_date}}]},
            no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(table, "User", documents_user)
    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng User: {e}")
        traceback.print_exc()
    finally:
        hbase_connection.close()
        mongo_client.close()
    logger.info("Chuyển dữ liệu User thành công từ MongoDB sang HBase.")


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


async def load_data_csc(start_date, end_date):
    try:
        node_name = "signservice_credential"
        await load_credential(node_name, start_date, end_date)
        await load_cert(node_name, start_date, end_date)
        await load_signature_transaction(node_name, start_date, end_date)
    except Exception as e:
        logger.error(f"Bỏ qua bảng do lỗi: {e}")
        traceback.print_exc()


async def load_credential(node_name, start_date, end_date):
    collection, table, hbase_connection, mongo_client = init_connect(config, node_name, "Credential", ["CREDENTIAL", "CREDENTIAL_STATUS_LOG"])
    try:
        documents = collection.find({'$and': [
            {'$or': [{'createdDate': {'$gte': start_date}}, {'modifiedDate': {'gte': start_date}}]},
            {'createdDate': {'$lt': end_date}}]},
            no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(table, "Credential", documents)

    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng Credential: {e} ")
        traceback.print_exc()
    finally:
        hbase_connection.close()
        mongo_client.close()
    logger.info("Chuyển dữ liệu Credential thành công từ MongoDB sang HBase.")


async def load_cert(node_name, start_date, end_date):
    collection_request_cert, table, hbase_connection, mongo_client = init_connect(config, node_name, "RequestCert", ["CERT"])
    try:
        documents_request_cert = collection_request_cert.find({'$and': [
            {'$or': [{'createdDate': {'$gte': start_date}}, {'updatedTime': {'gte': start_date}}]},
            {'createdDate': {'$lt': end_date}}]},
            no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(table, "RequestCert", documents_request_cert)
    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng RequestCert: {e}")
        traceback.print_exc()
    finally:
        hbase_connection.close()
        mongo_client.close()
    logger.info("Chuyển dữ liệu RequestCert thành công từ MongoDB sang HBase.")

    try:
        collection_cert, table, hbase_connection, mongo_client = init_connect(config, node_name, "Cert", ["CERT"])
        documents_cert = collection_cert.find({'$and': [
            {'createDate': {'$gte': start_date}},
            {'createDate': {'$lt': end_date}}]},
            no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(table, "Cert", documents_cert)
    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng Cert: {e}")
        traceback.print_exc()
    finally:
        hbase_connection.close()
        mongo_client.close()
    logger.info("Chuyển dữ liệu Cert thành công từ MongoDB sang HBase.")


async def _get_cut_off_name(node_name, table_name, start_date, end_date):
    all_data = [table_name]
    collection, table, hbase_connection, mongo_client = init_connect(config, node_name, "CutOff", None)
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
        logger.error(f"Lỗi khi xử lý bảng SignatureTransaction: {e}")
        traceback.print_exc()
    finally:
        mongo_client.close()
        return all_data


async def load_signature_transaction(node_name, start_date, end_date):
    try:
        table_name = "SignatureTransaction"
        table_names = await _get_cut_off_name(node_name, table_name, start_date, end_date)
        for name in table_names:
            collection, table, hbase_connection, mongo_client = init_connect(config, node_name, name, ["SIGNATURE_TRANSACTION"])
            documents = collection.find({'$and': [
                {'reqTime': {'$gte': start_date}},
                {'reqTime': {'$lt': end_date}}]},
                no_cursor_timeout=True).batch_size(CHUNK_SIZE)

            # Xử lý song song bất đồng bộ
            await _process_chunks(table, "SignatureTransaction", documents)
            hbase_connection.close()
            mongo_client.close()

    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng SignatureTransaction: {e}")
        traceback.print_exc()


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
        logger.error(f"Lỗi trong quá trình xử lý chunk: {e}")


async def _transfer_signature_transaction(chunk, tables):
    try:
        table = tables[0]
        batch = table.batch()
        for document in chunk:
            row_key = str(document["_id"])
            credentialId = str(document.get("credentialId", ""))
            serial = str(document.get("certSerial", ""))
            identityId = str(document.get("identityId", ""))
            uid = str(document.get("identityUid", ""))
            fullName = str(document.get("identityName", "")).encode('utf-8')
            email = str(document.get("identityEmail", ""))
            status = str(document.get("status", ""))
            statusDesc = str(document.get("statusDesc", ""))
            reqTime = str(int(document.get("reqTime", "").timestamp() * 1000))
            expiredTime = ""
            expireTime_check = document.get("expireTime", "")
            if expireTime_check is not None and expireTime_check != "" and expireTime_check != datetime(1, 1, 1, 0, 0):
                expiredTime = str(int(expireTime_check.timestamp() * 1000))
            finishDate = ""
            finishDate_check = document.get("finishDate", "")
            if finishDate_check is not None and finishDate_check != "" and finishDate_check != datetime(1, 1, 1, 0, 0):
                finishDate = str(int(finishDate_check.timestamp() * 1000))
            tranTypeDesc = str(document.get("tranTypeDesc", ""))
            tranType = str(document.get("tranType", ""))
            executeTime = str(document.get("excuteTime", ""))
            appId = str(document.get("appId", ""))
            appName = str(document.get("appName", "")).encode('utf-8')
            tranCode = str(document.get("tranCode", ""))
            data = {
                "INFO:SERIAL": serial,
                "INFO:CREDENTIAL_ID": credentialId,
                "INFO:IDENTITY_ID": identityId,
                "INFO:UID": uid,
                "INFO:FULL_NAME": fullName.encode('utf-8'),
                "INFO:EMAIL": email,
                "INFO:STATUS": str(status),
                "INFO:STATUS_DESC": statusDesc,
                "INFO:REQ_TIME": str(reqTime),
                "INFO:EXPIRED_TIME": str(expiredTime),
                "INFO:FINISH_DATE": str(finishDate),
                "INFO:TRAN_TYPE_DESC": tranTypeDesc,
                "INFO:TRAN_TYPE": str(tranType),
                "INFO:EXECUTE_TIME": str(executeTime),
                "INFO:APP_ID": appId,
                "INFO:APP_NAME": appName.encode('utf-8'),
                "INFO:TRAN_CODE": tranCode,
            }
            batch.put(row_key, data)
        batch.send()
        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {e}")
        traceback.print_exc()


async def _transfer_register(chunk, tables):
    try:
        table = tables[0]
        batch = table.batch()
        for document in chunk:
            row_key = str(document["_id"])
            status = document.get("status", "")
            statusDesc = document.get("statusDesc", "")
            modifiedDate = ""
            modifiedDate_check = document.get("modifiedDate", "")
            if modifiedDate_check is not None and modifiedDate_check != "" and modifiedDate_check != datetime(1, 1, 1, 0, 0):
                modifiedDate = int(modifiedDate_check.timestamp() * 1000)
            old_item = table.row(row_key)
            if old_item is None or old_item == {}:
                fullName = document.get("fullName", "").encode('utf-8')
                email = document.get("email", "")
                phone = document.get("phone", "")
                uid = document.get("uid", "")
                createDate = ""
                createDate_check = document.get("createdDate", "")
                if createDate_check is not None and createDate_check != "" and createDate_check != datetime(1, 1, 1, 0, 0):
                    createDate = int(createDate_check.timestamp() * 1000)
                source = document.get("source", "")
                if document.get('address'):
                    identityAddress = document.get('address', {})
                    provinceId = identityAddress.get("provinceId", "")
                    districtId = identityAddress.get("districtId", "")
                    wardId = identityAddress.get("wardId", "")
                    streetName = identityAddress.get("streetName", "")
                    address = identityAddress.get("address", "")
                else:
                    provinceId = ""
                    districtId = ""
                    wardId = ""
                    streetName = ""
                    address = ""

                # Ghi vào HBase
                table.put(row_key, {
                    "REGISTER:FULL_NAME": fullName,
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
                    table.put(row_key, {'REGISTER:STATUS': str(status)})
                if _column_value_exists(table, row_key, "REGISTER", "STATUS_DESC", statusDesc):
                    table.put(row_key, {'REGISTER:STATUS_DESC': str(statusDesc)})
                if _column_value_exists(table, row_key, "REGISTER", "MODIFIED_DATE", modifiedDate):
                    table.put(row_key, {'REGISTER:MODIFIED_DATE': str(modifiedDate)})
        batch.send()
        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {e}")
        traceback.print_exc()


async def _transfer_user(chunk, tables):
    try:
        table = tables[0]
        batch = table.batch()
        for document in chunk:
            row_key = str(document["_id"])
            status = document.get("status", "")
            statusDesc = document.get("statusDesc", "")
            modifiedDate = ""
            modifiedDate_check = document.get("modifiedDate", "")
            if modifiedDate_check is not None and modifiedDate_check != "" and modifiedDate_check != datetime(1, 1, 1, 0, 0):
                modifiedDate = int(modifiedDate_check.timestamp() * 1000)
            provinceCode = document.get("provinceCode", "")
            provinceCodes = document.get("provinceCodes", "")
            provinceCode_str = ", ".join(doc.get('name', '') for doc in provinceCodes if isinstance(doc, dict))
            role = document.get("roles", [])
            roles = ", ".join(doc.get('name', '') for doc in role if isinstance(doc, dict))
            preStatus = document.get("preStatus", "")

            old_item = table.row(row_key)
            if old_item is None or old_item == {}:
                uid = document.get("uid", "")
                username = document.get("username", "")
                fullName = document.get("fullName", "").encode('utf-8')
                email = document.get("email", "")
                phone = document.get("phone", "")
                localityId = document.get("localityId", "")
                uidPrefix = document.get("uidPrefix", "")
                userGroupId = document.get("userGroupId", "")
                createDate = int(document.get("createDate", "").timestamp() * 1000)
                accountType = document.get("accountType", "")
                accountTypeDesc = document.get("accountTypeDesc", "")
                isTest = document.get("isTest", "")
                isComAdmin = document.get("isComAdmin", "")
                source = document.get("source", "")

                if document.get('identityAddress'):
                    identityAddress = document.get('identityAddress', {})
                    provinceId = identityAddress.get("provinceId", "")
                    provinceName = identityAddress.get("provinceName", "")
                    districtId = identityAddress.get("districtId", "")
                    districtName = identityAddress.get("districtName", "")
                    wardId = identityAddress.get("wardId", "")
                    wardName = identityAddress.get("wardName", "")
                    streetName = identityAddress.get("streetName", "")
                    address = identityAddress.get("address", "")
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
                table.put(row_key, {
                    "INFO:UID": str(uid),
                    "INFO:USERNAME": str(username),
                    "INFO:FULL_NAME": fullName.encode('utf-8'),
                    "INFO:PHONE": str(phone),
                    "INFO:EMAIL": str(email),
                    "INFO:LOCALITY_CODE": str(localityId),
                    "INFO:UID_PREFIX": str(uidPrefix),
                    "INFO:USER_GROUP_ID": str(userGroupId),
                    "INFO:STATUS": str(status),
                    "INFO:STATUS_DESC": str(statusDesc),
                    "INFO:CREATE_DATE": str(createDate),
                    "INFO:MODIFIED_DATE": str(modifiedDate),
                    "INFO:ACCOUNT_TYPE": str(accountType),
                    "INFO:ACCOUNT_TYPE_DESC": str(accountTypeDesc),
                    "INFO:IS_COM_ADMIN": str(isComAdmin),
                    "INFO:IS_TEST": str(isTest),
                    "INFO:PRE_STATUS": str(preStatus),
                    "INFO:PROVINCE_CODE": str(provinceCode),
                    "INFO:PROVINCE_CODES": str(provinceCode_str),
                    "INFO:SOURCE": str(source),
                    "INFO:PROVINCE_ID": str(provinceId),
                    "INFO:PROVINCE_NAME": str(provinceName).encode('utf-8'),
                    "INFO:DISTRICT_ID": str(districtId),
                    "INFO:DISTRICT_NAME": str(districtName).encode('utf-8'),
                    "INFO:WARD_ID": str(wardId),
                    "INFO:WARD_NAME": str(wardName).encode('utf-8'),
                    "INFO:STREET_NAME": str(streetName).encode('utf-8'),
                    "INFO:ADDRESS": str(address).encode('utf-8'),
                    "INFO:ROLES": str(roles)
                })
            else:
                if _column_value_exists(table, row_key, "ÌNFO", "STATUS", status):
                    table.put(row_key, {'INFO:STATUS': str(status)})
                if _column_value_exists(table, row_key, "ÌNFO", "STATUS_DESC", statusDesc):
                    table.put(row_key, {'INFO:STATUS_DESC': str(statusDesc)})
                if _column_value_exists(table, row_key, "ÌNFO", "MODIFIED_DATE", modifiedDate):
                    table.put(row_key, {'INFO:MODIFIED_DATE': str(modifiedDate)})
                if _column_value_exists(table, row_key, "ÌNFO", "PROVINCE_CODE", provinceCode):
                    table.put(row_key, {'INFO:PROVINCE_CODE': str(provinceCode)})
                if _column_value_exists(table, row_key, "ÌNFO", "PROVINCE_CODES", provinceCodes):
                    table.put(row_key, {'INFO:PROVINCE_CODES': str(provinceCode_str)})
                if _column_value_exists(table, row_key, "ÌNFO", "ROLES", roles):
                    table.put(row_key, {'INFO:ROLES': str(roles)})
                if _column_value_exists(table, row_key, "ÌNFO", "PRE_STATUS", preStatus):
                    table.put(row_key, {'INFO:PRE_STATUS': str(preStatus)})

        batch.send()
        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {e}")
        traceback.print_exc()


async def _transfer_personal_sign_turn_order(chunk, tables):
    try:
        table = tables[0]
        batch = table.batch()
        for document in chunk:
            row_key = str(document["_id"])

            Status = document.get("Status", "")
            StatusDesc = document.get("StatusDesc", "")
            UpdatedDate = ""
            UpdatedDate_check = document.get("UpdatedDate", "")
            if UpdatedDate_check is not None and UpdatedDate_check != "" and UpdatedDate_check != datetime(1, 1, 1, 0, 0):
                UpdatedDate = int(UpdatedDate_check.timestamp() * 1000)
            PaymentStatus = document.get("PaymentStatus", "")
            PaymentStatusDesc = document.get("PaymentStatuDesc", "")
            TotalMoney = document.get("TotalMoney", "")
            Pricings = document.get('Pricings', [])
            if Pricings is not None and Pricings != []:
                Pricing = Pricings[-1]
                PricingName = Pricing.get("Name", "")
                PricingCode = Pricing.get("tocdo_id", "")
                Code = Pricing.get("Code", "")
                SignTurnNumber = Pricing.get("SignTurnNumber", "")
            else:
                PricingName = ""
                PricingCode = ""
                Code = ""
                SignTurnNumber = 0

            old_item = table.row(row_key)
            if old_item is None or old_item == {}:
                Indentity = document.get('UserInfo', {})
                IdentityId = Indentity.get('_id', '')
                Uid = Indentity.get("Uid", "")
                FullName = Indentity.get("FullName", "")
                LocalityCode = Indentity.get("LocalityCode", "")
                CreatedDate = int(document.get("CreatedDate", "").timestamp() * 1000)
                DHSXKDCustomerInfo = document.get("DHSXKDCustomerInfo", {})
                ma_tb = DHSXKDCustomerInfo.get("ma_tb", "")
                ma_gd = DHSXKDCustomerInfo.get("ma_gd", "")
                ma_kh = DHSXKDCustomerInfo.get("ma_kh", "")
                ma_hd = DHSXKDCustomerInfo.get("ma_hd", "")
                ma_hrm = DHSXKDCustomerInfo.get("ma_hrm", "")

                CredentialId = document.get("CredentialId", "")
                PaymentOrderId = document.get("PaymentOrderId", "")
                IsSyncDHSXKD = document.get("IsSyncDHSXKD", "")
                MaGt = document.get("MaGt", "")

                # Ghi vào HBase
                table.put(row_key, {
                    "INFO:IDENTITY_ID": str(IdentityId),
                    "INFO:UID": str(Uid),
                    "INFO:FULL_NAME": str(FullName).encode('utf-8'),
                    "INFO:LOCALITY_CODE": str(LocalityCode),
                    "INFO:STATUS": str(Status) if Status is not None else b"",
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
                    "INFO:PAYMENT_STATUS": str(PaymentStatus) if PaymentStatus is not None else b"",
                    "INFO:PAYMENT_STATUS_DESC": str(PaymentStatusDesc),
                    "INFO:IS_SYNC_DHSXKD": str(IsSyncDHSXKD),
                    "INFO:MA_GT": str(MaGt),
                    "INFO:PRICING_NAME": str(PricingName).encode('utf-8'),
                    "INFO:PRICING_CODE": str(PricingCode),
                    "INFO:CODE": str(Code),
                    "INFO:SIGN_TURN_NUMBER": str(SignTurnNumber),
                    "INFO:TOTAL_MONEY": str(TotalMoney) if TotalMoney is not None else b""
                })
            else:
                # Cập nhật các trường nếu có thay đổi
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
        logger.error(f"Lỗi trong quá trình xử lý batch: {e}")
        traceback.print_exc()


async def _transfer_request_cert(chunk, tables):
    keyword = "OPER"
    try:
        table = tables[0]
        batch = table.batch()
        for document in chunk:
            row_key = str(document["_id"])
            status = str(document.get("status", ""))
            statusDesc = str(document.get("statusDesc", ""))
            updatedTime = ""
            updatedTime_check = document.get("updatedTime", "")
            if updatedTime_check is not None and updatedTime_check != "" and updatedTime_check != datetime(1, 1, 1, 0, 0):
                updatedTime = str(int(updatedTime_check.timestamp() * 1000))
            approveTime = ""
            approveTime_check = document.get("approveTime", "")
            if approveTime_check is not None and approveTime_check != "" and approveTime_check != datetime(1, 1, 1, 0, 0):
                approveTime = str(int(approveTime_check.timestamp() * 1000))
            old_item = table.row(row_key)
            if old_item is None or old_item == {}:
                credentialId = str(document.get("credentialId", ""))

                clientId = str(document.get("createdByClientId", ""))
                clientName = str(document.get("createdByClientName", "")).encode('utf-8')
                createdDate = str(int(document.get("createdDate", "").timestamp() * 1000))
                pricingCode = str(document.get("pricingCode", ""))
                pricingName = str(document.get("pricingName", "")).encode('utf-8')
                username = str(document.get("username", ""))
                localityCode = ""
                if str.isspace(username):
                    index = username.index(keyword)
                    if index != -1:
                        localityCode = username[index:3]
                    else:
                        localityCode = localityCode[3:3]
                uid = str(document.get("uid", ""))
                identityId = str(document.get("identityId", ""))
                fullName = str(document.get("fullName", "")).encode('utf-8')
                period = str(document.get("period", ""))
                code = str(document.get("code", ""))
                requestType = str(document.get("requestType", ""))
                requestTypeDesc = str(document.get("requestTypeDesc", ""))
                # Ghi vào HBase
                table.put(row_key, {
                    "INFO:CREDENTIAL_ID": credentialId,
                    "INFO:STATUS": status,
                    "INFO:STATUS_DESC": statusDesc,
                    "INFO:CLIENT_ID": clientId,
                    "INFO:CLIENT_NAME": clientName,
                    "INFO:UPDATED_TIME": updatedTime,
                    "INFO:CREATED_DATE": createdDate,
                    "INFO:PRICING_CODE": pricingCode,
                    "INFO:PRICING_NAME": pricingName,
                    "INFO:USERNAME": username,
                    "INFO:LOCALITY_CODE": localityCode,
                    "INFO:UID": uid,
                    "INFO:IDENTITY_ID": identityId,
                    "INFO:FULL_NAME": fullName,
                    "INFO:PERIOD": period,
                    "INFO:CODE": code,
                    "INFO:APPROVE_TIME": approveTime,
                    "INFO:REQUEST_TYPE": requestType,
                    "INFO:REQUEST_TYPE_DESC": requestTypeDesc
                })
            else:
                if _column_value_exists(table, row_key, "INFO", "STATUS", status):
                    table.put(row_key, {'INFO:STATUS': status})
                if _column_value_exists(table, row_key, "INFO", "STATUS_DESC", statusDesc):
                    table.put(row_key, {'INFO:STATUS_DESC': statusDesc})
                if _column_value_exists(table, row_key, "INFO", "UPDATED_TIME", updatedTime):
                    table.put(row_key, {'INFO:UPDATED_TIME': updatedTime})
                if _column_value_exists(table, row_key, "INFO", "APPROVE_TIME", approveTime):
                    table.put(row_key, {'INFO:APPROVE_TIME': approveTime})

        batch.send()
        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {e}")
        traceback.print_exc()


async def _transfer_cert(chunk, tables):
    try:
        table = tables[0]
        batch = table.batch()
        for document in chunk:
            row_key = str(document["requestId"])
            serial = str(document.get("serial", ""))
            status = str(document.get("status", ""))
            statusDesc = str(document.get("statusDesc", ""))
            subject = str(document.get("subject", ""))
            validFrom = str(int(document.get("validFrom", "").timestamp() * 1000))
            validTo = str(int(document.get("validTo", "").timestamp() * 1000))
            createdDate = str(int(document.get("createDate", "").timestamp() * 1000))
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
        logger.error(f"Lỗi trong quá trình xử lý batch: {e}")
        traceback.print_exc()


async def _transfer_credential(chunk, tables):
    try:
        table_credential = tables[0]
        table_log = tables[1]
        batch_credential = table_credential.batch()
        batch_log = table_log.batch()
        keyword = "OPER"
        pricing_ps0 = {
            "Chứng thư số cá nhân trên các ứng dụng",
            "SmartCA cá nhân PS0 (Công dân)",
            "SmartCA Personal App Sign Limit Std"
        }
        date_now = datetime.now()
        temp_time = f"{date_now.year}-{date_now.month}-{date_now.day - 1}T17:00:00.000Z"
        end_date = datetime.strptime(temp_time, "%Y-%m-%dT%H:%M:%S.%fZ")
        date_str = f"{end_date.year}-{end_date.month:02}-{end_date.day:02}"
        for document in chunk:
            row_key = str(document["_id"])
            fullName = document.get("identity", {}).get("name", "")
            if fullName is not None and ("test" in fullName or "Test" in fullName or "TEST" in fullName):
                continue
            pricingName = document.get("contract", "").get("servicePack", "")
            if pricingName is not None and ("test" in pricingName or "Test" in pricingName or "TEST" in pricingName):
                continue
            subjectDN = document.get("subjectDN", "")
            pattern_1 = "CN=[^,]+ Test,"
            pattern_2 = "CN=[^,]+ TEST,"
            pattern_3 = "CN=[^,]+ test,"
            if subjectDN is not None and (re.search(pattern_1, subjectDN) or re.search(pattern_2, subjectDN) or re.search(pattern_3, subjectDN)):
                continue

            status = document.get("status", "")
            statusDesc = document.get("statusDesc", "")
            modifiedDate = ""
            modifiedDate_check = document.get("modifiedDate", "")
            if modifiedDate_check is not None and modifiedDate_check != "" and modifiedDate_check != datetime(1, 1, 1, 0, 0):
                modifiedDate = int(modifiedDate_check.timestamp() * 1000)
            certs = document.get('certs', [])
            if certs is not None and certs is not [] and certs:
                cert = certs[-1]
                certStatus = cert.get("status", "")
                certStatusDesc = cert.get("statusDesc", "")
                validFrom = str(int(cert.get("validFrom", "").timestamp() * 1000))
                validTo_temp = cert.get("validTo", "")
                if validTo_temp < end_date:
                    certStatus = '4'
                    certStatusDesc = "EXPIRED"
                validTo = str(int(validTo_temp.timestamp() * 1000))
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

                createdDate = int(document.get("createdDate", "").timestamp() * 1000)
                identityId = document.get("identity", "").get("_id", "")

                email = document.get("identity", {}).get("email", "")
                phone = document.get("identity", {}).get("phone", "")
                username = document.get("identity", {}).get("username", "")
                uid = document.get("identity", {}).get("uid", "")
                clientId = document.get("identity", {}).get("createdByClientId", "")
                clientName = document.get("identity", {}).get("createdByClientName", "")
                localityCode = document.get("identity", {}).get("LocalityCode", "")
                if localityCode is not None and str.isspace(localityCode) and username is not None and str.isspace(username) is False:
                    index = username.index(keyword)
                    if index != -1:
                        localityCode = username[index:3]
                    else:
                        localityCode = localityCode[3:3]
                ma_tb = document.get("identity", {}).get("DHSXKDSubcriptionCode", "")
                source = document.get("identity", {}).get("source", "")

                contractNumber = document.get("contract", {}).get("number", "")

                validity = document.get("contract", "").get("validity", "")
                pricingCode = document.get("contract", "").get("pricingCode", "")
                if pricingCode is not None and str.isspace(pricingCode) and pricingName in pricing_ps0:
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

            else:
                if _column_value_exists(table_credential, row_key, "INFO", "STATUS", status):
                    table_credential.put(row_key, {'INFO:STATUS': str(status)})
                if _column_value_exists(table_credential, row_key, "INFO", "SUBJECT_DN", subjectDN):
                    table_credential.put(row_key, {'INFO:SUBJECT_DN': str(subjectDN)})
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
        batch_log.send()
        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {e}")
        traceback.print_exc()


def _column_value_exists(table, row_key, column_family, column_name, new_value):
    # Lấy giá trị hiện tại của cột
    current_value = table.row(row_key).get(f'{column_family}:{column_name}')
    check = current_value is not None and current_value != new_value and current_value != ''
    # So sánh giá trị hiện tại với giá trị mới
    return check


def _convert_datetime(input_datetime):
    if input_datetime and input_datetime != datetime(1, 1, 1, 0, 0):
        return str(int(input_datetime.timestamp() * 1000))
    return ''


def _convert_number(input_int):
    if input_int:
        return str(input_int)
    return ''


def _convert_boolean(input_boolean):
    if input_boolean:
        return str(int(input_boolean))
    return ''
