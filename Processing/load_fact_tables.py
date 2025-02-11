import asyncio
import json
import re
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import happybase
import pymongo

CHUNK_SIZE = 1000
MAX_WORKERS = 4


def _init_connect(task_config, node_name, collection_name, table_name):
    mongo = task_config["mongo"]
    mongo_uri = mongo["identity_uri"]
    mongo_client = pymongo.MongoClient(mongo_uri)
    mongodb = mongo_client[node_name]
    collection = mongodb[collection_name]
    if table_name is None:
        return collection, None, None, mongo_client
    hbase = task_config["hbase"]
    hbase_uri = hbase["uri"]
    hbase_connection = happybase.Connection(hbase_uri)
    table = hbase_connection.table(table_name)
    return collection, table, hbase_connection, mongo_client


async def process_fetch_tables(task_file):
    with open(task_file, 'r') as f:
        task_config = json.load(f)
    date_now = datetime.now()
    temp_time = f"{date_now.year}-{date_now.month}-{date_now.day - 1}T17:00:00.000Z"
    end_date = datetime.strptime(temp_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    start_date = end_date - timedelta(days=365)
    await load_data_identity(task_config, start_date, end_date)
    await load_data_csc(task_config, start_date, end_date)


async def load_data_identity(task_config, start_date, end_date):
    try:
        node_name = "signservice_identity"
        await load_user(task_config, node_name, start_date, end_date)
        await load_cert_order(task_config, node_name, start_date, end_date)
        await load_personal_turn_order(task_config, node_name, start_date, end_date)
    except Exception as e:
        print(f"Bỏ qua bảng do lỗi: {e}")
        traceback.print_exc()


async def load_cert_order(task_config, node_name, start_date, end_date):
    collection, table, hbase_connection, mongo_client = _init_connect(task_config, node_name, "CertOrder", "CertOrder")
    try:
        documents = collection.find({'$and': [
            {'$or': [{'CreatedDate': {'$gte': start_date}}, {'UpdatedDate': {'gte': start_date}}]},
            {'CreatedDate': {'$lt': end_date}}]},
            no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(table, "CertOrder", documents)
    except Exception as e:
        print(f"Lỗi khi xử lý bảng CertOrder: {e} ")
        traceback.print_exc()
    finally:
        hbase_connection.close()
        mongo_client.close()
    print("Chuyển dữ liệu CertOrder thành công từ MongoDB sang HBase.")


async def _transfer_cert_order(chunk, table):
    batch = table.batch()
    try:
        for document in chunk:
            FullName = document.get("FullName", "")
            if FullName is not None and ("test" in FullName or "Test" in FullName or "TEST" in FullName):
                continue
            Pricing = document.get("Pricing", {})
            if Pricing is None or Pricing is {}:
                continue
            PricingName = Pricing.get("PricingName", "")
            if PricingName is not None and ("test" in PricingName or "Test" in PricingName or "TEST" in PricingName):
                continue
            DHSXKDCustomerInfo = document.get("DHSXKDCustomerInfo", {})
            if DHSXKDCustomerInfo is None or DHSXKDCustomerInfo is {}:
                continue
            ma_gd = DHSXKDCustomerInfo.get("ma_gd", "")
            if ma_gd is not None and ("test" in ma_gd or "Test" in ma_gd or "TEST" in ma_gd):
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
                    "info:identity_id": IdentityId,
                    "info:uid": str(Uid).encode('utf-8'),
                    "info:full_name": str(FullName).encode('utf-8'),
                    "info:email": str(Email).encode('utf-8'),
                    "info:phone": str(Phone).encode('utf-8'),
                    "info:locality_code": str(LocalityCode).encode('utf-8'),
                    "info:type": str(Type).encode('utf-8'),
                    "info:type_desc": str(TypeDesc).encode('utf-8'),
                    "info:status": str(Status).encode('utf-8'),
                    "info:status_desc": str(StatusDesc).encode('utf-8'),
                    "info:client_name": str(ClientName).encode('utf-8'),
                    "info:client_id": str(ClientId).encode('utf-8'),
                    "info:created_date": str(CreatedDate).encode('utf-8'),
                    "info:updated_date": str(UpdatedDate).encode('utf-8'),
                    "info:ma_tb": str(ma_tb).encode('utf-8'),
                    "info:ma_gd": str(ma_gd).encode('utf-8'),
                    "info:ma_kh": str(ma_kh).encode('utf-8'),
                    "info:ma_hd": str(ma_hd).encode('utf-8'),
                    "info:ma_hrm": str(ma_hrm).encode('utf-8'),
                    "info:address": str(address).encode('utf-8'),
                    "info:source": str(Source).encode('utf-8'),
                    "info:request_cert_id": str(RequestCertId).encode('utf-8'),
                    "info:serial": str(Serial).encode('utf-8'),
                    "info:credential_id": str(CredentialId).encode('utf-8'),
                    "info:pricing_code": str(PricingCode).encode('utf-8'),
                    "info:pricing_name": str(PricingName).encode('utf-8'),
                    "info:pricing_price": str(Price).encode('utf-8'),
                    "info:code": str(Code).encode('utf-8'),
                    "info:sign_type": str(SignType).encode('utf-8'),
                    "info:validity": str(Validity).encode('utf-8'),
                    "info:ma_gt": str(MaGt).encode('utf-8'),
                    "info:previous_serial": str(PreviousSerial).encode('utf-8'),
                    "info:contract_url": str(ContractUrl).encode('utf-8'),
                    "info:acceptance_url": str(AcceptanceUrl).encode('utf-8'),
                    "info:log_content": str(LogContent).encode('utf-8'),
                    "info:log_created_date": str(LogCreatedDate).encode('utf-8'),

                    "address:province_id": str(provinceId).encode('utf-8'),
                    "address:province_name": str(provinceName).encode('utf-8'),
                    "address:district_id": str(districtId).encode('utf-8'),
                    "address:district_name": str(districtName).encode('utf-8'),
                    "address:ward_id": str(wardId).encode('utf-8'),
                    "address:ward_name": str(wardName).encode('utf-8'),
                    "address:street_name": str(streetName).encode('utf-8'),
                    "address:address_detail": str(address).encode('utf-8'),
                }
                batch.put(row_key, data)
            else:
                if _column_value_exists(table, row_key, "ìnfo", "status", Status):
                    table.put(row_key, {'info:status': str(Status).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "status_desc", StatusDesc):
                    table.put(row_key, {'info:status_desc': str(StatusDesc).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "updated_date", UpdatedDate):
                    table.put(row_key, {'info:updated_date': str(UpdatedDate).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "request_cert_id", RequestCertId):
                    table.put(row_key, {'info:request_cert_id': str(RequestCertId).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "credential_id", CredentialId):
                    table.put(row_key, {'info:credential_id': str(CredentialId).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "contract_url", ContractUrl):
                    table.put(row_key, {'info:contract_url': str(ContractUrl).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "acceptance_url", AcceptanceUrl):
                    table.put(row_key, {'info:acceptance_url': str(AcceptanceUrl).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "log_content", LogContent):
                    table.put(row_key, {'info:log_content': str(LogContent).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "log_created_date", LogCreatedDate):
                    table.put(row_key, {'info:log_created_date': str(LogCreatedDate).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "serial", Serial):
                    table.put(row_key, {'info:serial': str(Serial).encode('utf-8')})
            batch.send()
        print(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        print(f"Lỗi trong quá trình xử lý batch: {e}")
        traceback.print_exc()


async def load_user(task_config, node_name, start_date, end_date):
    collection_register, table, hbase_connection, mongo_client = _init_connect(task_config, node_name, "Register", "User")
    try:
        documents_register = collection_register.find({'$and': [
            {'$or': [{'createdDate': {'$gte': start_date}}, {'modifiedDate': {'gte': start_date}}]},
            {'createdDate': {'$lt': end_date}}]},
            no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(table, "Register", documents_register)
    except Exception as e:
        print(f"Lỗi khi xử lý bảng Register: {e}")
        traceback.print_exc()
    finally:
        hbase_connection.close()
        mongo_client.close()
    print("Chuyển dữ liệu Register thành công từ MongoDB sang HBase.")

    collection_user, table, hbase_connection, mongo_client = _init_connect(task_config, node_name, "User", "User")
    try:
        documents_user = collection_user.find({'$and': [
            {'$or': [{'createDate': {'$gte': start_date}}, {'modifiedDate': {'gte': start_date}}]},
            {'createDate': {'$lt': end_date}}]},
            no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(table, "User", documents_user)
    except Exception as e:
        print(f"Lỗi khi xử lý bảng User: {e}")
        traceback.print_exc()
    finally:
        hbase_connection.close()
        mongo_client.close()
    print("Chuyển dữ liệu User thành công từ MongoDB sang HBase.")


async def load_personal_turn_order(task_config, node_name, start_date, end_date):
    collection, table, hbase_connection, mongo_client = _init_connect(task_config, node_name, "PersonalSignTurnOrder", "PersonalSignTurnOrder")
    try:
        documents = collection.find({'$and': [
            {'$or': [{'CreatedDate': {'$gte': start_date}}, {'UpdatedDate': {'gte': start_date}}]},
            {'CreatedDate': {'$lt': end_date}}]},
            no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(table, "PersonalSignTurnOrder", documents)
    except Exception as e:
        print(f"Lỗi khi xử lý bảng PersonalSignTurnOrder: {e}")
        traceback.print_exc()
    finally:
        hbase_connection.close()
        mongo_client.close()
    print("Chuyển dữ liệu PersonalSignTurnOrder thành công từ MongoDB sang HBase.")


async def load_data_csc(task_config, start_date, end_date):
    try:
        node_name = "signservice_credential"
        await load_credential(task_config, node_name, start_date, end_date)
        await load_cert(task_config, node_name, start_date, end_date)
        await load_signature_transaction(task_config, node_name, start_date, end_date)
    except Exception as e:
        print(f"Bỏ qua bảng do lỗi: {e}")
        traceback.print_exc()


async def load_credential(task_config, node_name, start_date, end_date):
    collection, table, hbase_connection, mongo_client = _init_connect(task_config, node_name, "Credential", "Credential")
    try:
        documents = collection.find({'$and': [
            {'$or': [{'createdDate': {'$gte': start_date}}, {'modifiedDate': {'gte': start_date}}]},
            {'createdDate': {'$lt': end_date}}]},
            no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(table, "Credential", documents)

    except Exception as e:
        print(f"Lỗi khi xử lý bảng Credential: {e} ")
        traceback.print_exc()
    finally:
        hbase_connection.close()
        mongo_client.close()
    print("Chuyển dữ liệu Credential thành công từ MongoDB sang HBase.")


async def load_cert(task_config, node_name, start_date, end_date):
    collection_request_cert, table, hbase_connection, mongo_client = _init_connect(task_config, node_name, "RequestCert", "Cert")
    try:
        documents_request_cert = collection_request_cert.find({'$and': [
            {'$or': [{'createdDate': {'$gte': start_date}}, {'updatedTime': {'gte': start_date}}]},
            {'createdDate': {'$lt': end_date}}]},
            no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(table, "RequestCert", documents_request_cert)
    except Exception as e:
        print(f"Lỗi khi xử lý bảng RequestCert: {e}")
        traceback.print_exc()
    finally:
        hbase_connection.close()
        mongo_client.close()
    print("Chuyển dữ liệu RequestCert thành công từ MongoDB sang HBase.")

    try:
        collection_cert, table, hbase_connection, mongo_client = _init_connect(task_config, node_name, "Cert", "Cert")
        documents_cert = collection_cert.find({'$and': [
            {'createDate': {'$gte': start_date}},
            {'createDate': {'$lt': end_date}}]},
            no_cursor_timeout=True).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(table, "Cert", documents_cert)
    except Exception as e:
        print(f"Lỗi khi xử lý bảng Cert: {e}")
        traceback.print_exc()
    finally:
        hbase_connection.close()
        mongo_client.close()
    print("Chuyển dữ liệu Cert thành công từ MongoDB sang HBase.")


async def _get_cut_off_name(task_config, node_name, table_name, start_date, end_date):
    all_data = [table_name]
    collection, table, hbase_connection, mongo_client = _init_connect(task_config, node_name, "CutOff", None)
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
        print(f"Get table name cut off table {table_name} from MongoDB.")
    except Exception as e:
        print(f"Lỗi khi xử lý bảng SignatureTransaction: {e}")
        traceback.print_exc()
    finally:
        mongo_client.close()
        return all_data


async def load_signature_transaction(task_config, node_name, start_date, end_date):
    try:
        table_name = "SignatureTransaction"
        table_names = await _get_cut_off_name(task_config, node_name, table_name, start_date, end_date)
        for name in table_names:
            collection, table, hbase_connection, mongo_client = _init_connect(task_config, node_name, name, "SignatureTransaction")
            documents = collection.find({'$and': [
                {'reqTime': {'$gte': start_date}},
                {'reqTime': {'$lt': end_date}}]},
                no_cursor_timeout=True).batch_size(CHUNK_SIZE)

            # Xử lý song song bất đồng bộ
            await _process_chunks(table, "SignatureTransaction", documents)
            hbase_connection.close()
            mongo_client.close()

    except Exception as e:
        print(f"Lỗi khi xử lý bảng SignatureTransaction: {e}")
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
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            tasks = [
                await asyncio.to_thread(_transfer_chunk_sync, chunk, table, collection_name)
                for chunk in chunks
            ]
            if tasks:
                await asyncio.gather(*tasks)

        print(f"Chuyển dữ liệu {collection_name} thành công từ MongoDB sang HBase.{i + 1}")
    except Exception as e:
        print(f"Lỗi khi xử lý bảng {collection_name}: {e}")
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
                print(f"Bảng {collection_name} không được nhận diện.")
                return None
    except Exception as e:
        print(f"Lỗi trong quá trình xử lý chunk: {e}")


async def _transfer_signature_transaction(chunk, table):
    try:
        batch = table.batch()
        for document in chunk:
            row_key = str(document["_id"])
            credentialId = str(document.get("credentialId", "")).encode('utf-8')
            serial = str(document.get("certSerial", "")).encode('utf-8')
            identityId = str(document.get("identityId", "")).encode('utf-8')
            uid = str(document.get("identityUid", "")).encode('utf-8')
            fullName = str(document.get("identityName", "")).encode('utf-8')
            email = str(document.get("identityEmail", "")).encode('utf-8')
            status = str(document.get("status", "")).encode('utf-8')
            statusDesc = str(document.get("statusDesc", "")).encode('utf-8')
            reqTime = str(int(document.get("reqTime", "").timestamp() * 1000)).encode('utf-8')
            expiredTime = ""
            expireTime_check = document.get("expireTime", "")
            if expireTime_check is not None and expireTime_check != "" and expireTime_check != datetime(1, 1, 1, 0, 0):
                expiredTime = str(int(expireTime_check.timestamp() * 1000)).encode('utf-8')
            finishDate = ""
            finishDate_check = document.get("finishDate", "")
            if finishDate_check is not None and finishDate_check != "" and finishDate_check != datetime(1, 1, 1, 0, 0):
                finishDate = str(int(finishDate_check.timestamp() * 1000)).encode('utf-8')
            tranTypeDesc = str(document.get("tranTypeDesc", "")).encode('utf-8')
            tranType = str(document.get("tranType", "")).encode('utf-8')
            executeTime = str(document.get("excuteTime", "")).encode('utf-8')
            appId = str(document.get("appId", "")).encode('utf-8')
            appName = str(document.get("appName", "")).encode('utf-8')
            tranCode = str(document.get("tranCode", "")).encode('utf-8')
            data = {
                "info:serial": serial,
                "info:credential_id": credentialId,
                "info:identity_id": identityId,
                "info:uid": uid,
                "info:full_name": fullName,
                "info:email": email,
                "info:status": status,
                "info:status_desc": statusDesc,
                "info:req_time": reqTime,
                "info:expired_time": expiredTime,
                "info:finish_date": finishDate,
                "info:tran_type_desc": tranTypeDesc,
                "info:tran_type": tranType,
                "info:execute_time": executeTime,
                "info:app_id": appId,
                "info:app_name": appName,
                "info:tran_code": tranCode,

            }
            batch.put(row_key, data)
        batch.send()
        print(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        print(f"Lỗi trong quá trình xử lý batch: {e}")
        traceback.print_exc()


async def _transfer_register(chunk, table):
    try:
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
                    "register:full_name": fullName,
                    "register:uid": str(uid).encode('utf-8'),
                    "register:phone": str(phone).encode('utf-8'),
                    "register:email": str(email).encode('utf-8'),
                    "register:status": str(status).encode('utf-8'),
                    "register:status_desc": str(statusDesc).encode('utf-8'),
                    "register:create_date": str(createDate).encode('utf-8'),
                    "register:modified_date": str(modifiedDate).encode('utf-8'),
                    "register:source": str(source).encode('utf-8'),
                    "register:province_id": str(provinceId).encode('utf-8'),
                    "register:district_id": str(districtId).encode('utf-8'),
                    "register:ward_id": str(wardId).encode('utf-8'),
                    "register:street_name": str(streetName).encode('utf-8'),
                    "register:address": str(address)
                })
            else:
                if _column_value_exists(table, row_key, "register", "status", status):
                    table.put(row_key, {'register:status': str(status).encode('utf-8')})
                if _column_value_exists(table, row_key, "register", "status_desc", statusDesc):
                    table.put(row_key, {'register:status_desc': str(statusDesc).encode('utf-8')})
                if _column_value_exists(table, row_key, "register", "modified_date", modifiedDate):
                    table.put(row_key, {'register:modified_date': str(modifiedDate).encode('utf-8')})
        batch.send()
        print(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        print(f"Lỗi trong quá trình xử lý batch: {e}")
        traceback.print_exc()


async def _transfer_user(chunk, table):
    try:
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
                    "info:uid": str(uid).encode('utf-8'),
                    "info:username": str(username).encode('utf-8'),
                    "info:full_name": fullName,
                    "info:phone": str(phone).encode('utf-8'),
                    "info:email": str(email).encode('utf-8'),
                    "info:locality_code": str(localityId).encode('utf-8'),
                    "info:uid_prefix": str(uidPrefix).encode('utf-8'),
                    "info:user_group_id": str(userGroupId).encode('utf-8'),
                    "info:status": str(status).encode('utf-8'),
                    "info:status_desc": str(statusDesc).encode('utf-8'),
                    "info:create_date": str(createDate).encode('utf-8'),
                    "info:modified_date": str(modifiedDate).encode('utf-8'),
                    "info:account_type": str(accountType).encode('utf-8'),
                    "info:account_type_desc": str(accountTypeDesc).encode('utf-8'),
                    "info:is_com_admin": str(isComAdmin).encode('utf-8'),
                    "info:is_test": str(isTest).encode('utf-8'),
                    "info:pre_status": str(preStatus).encode('utf-8'),
                    "info:province_code": str(provinceCode).encode('utf-8'),
                    "info:province_codes": str(provinceCode_str),
                    "info:source": str(source).encode('utf-8'),
                    "info:province_id": str(provinceId).encode('utf-8'),
                    "info:province_name": str(provinceName).encode('utf-8'),
                    "info:district_id": str(districtId).encode('utf-8'),
                    "info:district_name": str(districtName),
                    "info:ward_id": str(wardId).encode('utf-8'),
                    "info:ward_name": str(wardName).encode('utf-8'),
                    "info:street_name": str(streetName).encode('utf-8'),
                    "info:address": str(address),
                    "info:roles": str(roles)
                })
            else:
                if _column_value_exists(table, row_key, "ìnfo", "status", status):
                    table.put(row_key, {'info:status': str(status).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "status_desc", statusDesc):
                    table.put(row_key, {'info:status_desc': str(statusDesc).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "modified_date", modifiedDate):
                    table.put(row_key, {'info:modified_date': str(modifiedDate).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "province_code", provinceCode):
                    table.put(row_key, {'info:province_code': str(provinceCode).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "province_codes", provinceCodes):
                    table.put(row_key, {'info:province_codes': str(provinceCode_str).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "roles", roles):
                    table.put(row_key, {'info:roles': str(roles).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "pre_status", preStatus):
                    table.put(row_key, {'info:pre_status': str(preStatus).encode('utf-8')})
        batch.send()
        print(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        print(f"Lỗi trong quá trình xử lý batch: {e}")
        traceback.print_exc()


async def _transfer_personal_sign_turn_order(chunk, table):
    try:
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
            PaymentStatusDesc = document.get("PaymentStatusDesc", "")
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
                if DHSXKDCustomerInfo is not None and DHSXKDCustomerInfo != {}:
                    ma_tb = DHSXKDCustomerInfo.get("ma_tb", "")
                    ma_gd = DHSXKDCustomerInfo.get("ma_gd", "")
                    ma_kh = DHSXKDCustomerInfo.get("ma_kh", "")
                    ma_hd = DHSXKDCustomerInfo.get("ma_hd", "")
                    ma_hrm = DHSXKDCustomerInfo.get("ma_hrm", "")
                else:
                    ma_tb = ""
                    ma_gd = ""
                    ma_kh = ""
                    ma_hd = ""
                    ma_hrm = ""
                CredentialId = document.get("CredentialId", "")
                PaymentOrderId = document.get("PaymentOrderId", "")
                IsSyncDHSXKD = document.get("IsSyncDHSXKD", "")
                MaGt = document.get("MaGt", "")

                # Ghi vào HBase
                table.put(row_key, {
                    "info:identity_id": str(IdentityId).encode('utf-8'),
                    "info:uid": str(Uid).encode('utf-8'),
                    "info:full_name": str(FullName).encode('utf-8'),
                    "info:locality_code": str(LocalityCode).encode('utf-8'),
                    "info:status": str(Status).encode('utf-8'),
                    "info:status_desc": str(StatusDesc).encode('utf-8'),
                    "info:created_date": str(CreatedDate).encode('utf-8'),
                    "info:updated_date": str(UpdatedDate).encode('utf-8'),
                    "info:ma_tb": str(ma_tb).encode('utf-8'),
                    "info:ma_gd": str(ma_gd).encode('utf-8'),
                    "info:ma_kh": str(ma_kh).encode('utf-8'),
                    "info:ma_hd": str(ma_hd).encode('utf-8'),
                    "info:ma_hrm": str(ma_hrm).encode('utf-8'),
                    "info:credential_id": str(CredentialId).encode('utf-8'),
                    "info:payment_order_id": str(PaymentOrderId).encode('utf-8'),
                    "info:payment_status": str(PaymentStatus).encode('utf-8'),
                    "info:payment_status_desc": str(PaymentStatusDesc).encode('utf-8'),
                    "info:is_sync_dhsxkd": str(IsSyncDHSXKD).encode('utf-8'),
                    "info:ma_gt": str(MaGt).encode('utf-8'),
                    "info:pricing_name": str(PricingName).encode('utf-8'),
                    "info:pricing_code": str(PricingCode).encode('utf-8'),
                    "info:code": str(Code).encode('utf-8'),
                    "info:sign_turn_number": str(SignTurnNumber).encode('utf-8'),
                    "info:total_money": str(TotalMoney).encode('utf-8')
                })
            else:
                if _column_value_exists(table, row_key, "ìnfo", "status", Status):
                    table.put(row_key, {'info:status': str(Status).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "status_desc", StatusDesc):
                    table.put(row_key, {'info:status_desc': str(StatusDesc).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "updated_date", UpdatedDate):
                    table.put(row_key, {'info:updated_date': str(UpdatedDate).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "payment_status", PaymentStatus):
                    table.put(row_key, {'info:payment_status': str(PaymentStatus).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "payment_status_desc", PaymentStatusDesc):
                    table.put(row_key, {'info:payment_status_desc': str(PaymentStatusDesc).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "pricing_name", PricingName):
                    table.put(row_key, {'info:pricing_name': str(PricingName).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "pricing_code", PricingCode):
                    table.put(row_key, {'info:pricing_code': str(PricingCode).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "code", Code):
                    table.put(row_key, {'info:code': str(Code).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "sign_turn_number", SignTurnNumber):
                    table.put(row_key, {'info:sign_turn_number': str(SignTurnNumber).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "total_money", TotalMoney):
                    table.put(row_key, {'info:total_money': str(TotalMoney).encode('utf-8')})
        batch.send()
        print(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        print(f"Lỗi trong quá trình xử lý batch: {e}")
        traceback.print_exc()


async def _transfer_request_cert(chunk, table):
    keyword = "OPER"
    try:
        batch = table.batch()
        for document in chunk:
            row_key = str(document["_id"]).encode('utf-8')
            status = str(document.get("status", "")).encode('utf-8')
            statusDesc = str(document.get("statusDesc", "")).encode('utf-8')
            updatedTime = ""
            updatedTime_check = document.get("updatedTime", "")
            if updatedTime_check is not None and updatedTime_check != "" and updatedTime_check != datetime(1, 1, 1, 0, 0):
                updatedTime = str(int(updatedTime_check.timestamp() * 1000)).encode('utf-8')
            approveTime = ""
            approveTime_check = document.get("approveTime", "")
            if approveTime_check is not None and approveTime_check != "" and approveTime_check != datetime(1, 1, 1, 0, 0):
                approveTime = str(int(approveTime_check.timestamp() * 1000)).encode('utf-8')
            old_item = table.row(row_key)
            if old_item is None or old_item == {}:
                credentialId = str(document.get("credentialId", "")).encode('utf-8')

                clientId = str(document.get("createdByClientId", "")).encode('utf-8')
                clientName = str(document.get("createdByClientName", "")).encode('utf-8')
                createdDate = str(int(document.get("createdDate", "").timestamp() * 1000)).encode('utf-8')
                pricingCode = str(document.get("pricingCode", "")).encode('utf-8')
                pricingName = str(document.get("pricingName", "")).encode('utf-8')
                username = str(document.get("username", ""))
                localityCode = ""
                if str.isspace(username):
                    index = username.index(keyword)
                    if index != -1:
                        localityCode = username[index:3]
                    else:
                        localityCode = localityCode[3:3]
                uid = str(document.get("uid", "")).encode('utf-8')
                identityId = str(document.get("identityId", "")).encode('utf-8')
                fullName = str(document.get("fullName", "")).encode('utf-8')
                period = str(document.get("period", "")).encode('utf-8')
                code = str(document.get("code", "")).encode('utf-8')
                requestType = str(document.get("requestType", "")).encode('utf-8')
                requestTypeDesc = str(document.get("requestTypeDesc", "")).encode('utf-8')
                # Ghi vào HBase
                table.put(row_key, {
                    "info:credential_id": credentialId,
                    "info:status": status,
                    "info:status_desc": statusDesc,
                    "info:client_id": clientId,
                    "info:client_name": clientName,
                    "info:updated_time": updatedTime,
                    "info:created_date": createdDate,
                    "info:pricing_code": pricingCode,
                    "info:pricing_name": pricingName,
                    "info:username": username,
                    "info:locality_code": localityCode,
                    "info:uid": uid,
                    "info:identity_id": identityId,
                    "info:full_name": fullName,
                    "info:period": period,
                    "info:code": code,
                    "info:approve_time": approveTime,
                    "info:request_type": requestType,
                    "info:request_type_desc": requestTypeDesc
                })
            else:
                if _column_value_exists(table, row_key, "ìnfo", "status", status):
                    table.put(row_key, {'info:status': status})
                if _column_value_exists(table, row_key, "ìnfo", "status_desc", statusDesc):
                    table.put(row_key, {'info:status_desc': statusDesc})
                if _column_value_exists(table, row_key, "ìnfo", "updated_time", updatedTime):
                    table.put(row_key, {'info:updated_time': updatedTime})
                if _column_value_exists(table, row_key, "ìnfo", "approve_time", approveTime):
                    table.put(row_key, {'info:approve_time': approveTime})
        batch.send()
        print(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        print(f"Lỗi trong quá trình xử lý batch: {e}")
        traceback.print_exc()


async def _transfer_cert(chunk, table):
    try:
        batch = table.batch()
        for document in chunk:
            row_key = str(document["requestId"])
            serial = str(document.get("serial", "")).encode('utf-8')
            status = str(document.get("status", "")).encode('utf-8')
            statusDesc = str(document.get("statusDesc", "")).encode('utf-8')
            subject = str(document.get("subject", "")).encode('utf-8')
            validFrom = str(int(document.get("validFrom", "").timestamp() * 1000))
            validTo = str(int(document.get("validTo", "").timestamp() * 1000))
            createdDate = str(int(document.get("createDate", "").timestamp() * 1000))
            # Ghi vào HBase
            table.put(row_key, {
                "info:cert_status": status,
                "info:cert_status_desc": statusDesc,
                "info:serial": serial,
                "info:subject": subject,
                "info:valid_from": validFrom,
                "info:valid_to": validTo,
                "info:cert_created_date": createdDate,
            })
        batch.send()
        print(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        print(f"Lỗi trong quá trình xử lý batch: {e}")
        traceback.print_exc()


async def _transfer_credential(chunk, table):
    try:
        batch = table.batch()
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

            old_item = table.row(row_key)
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
                table.put(row_key, {
                    "info:subject_dn": str(subjectDN).encode('utf-8'),
                    "info:status": str(status).encode('utf-8'),
                    "info:status_desc": str(statusDesc).encode('utf-8'),
                    "info:created_date": str(createdDate).encode('utf-8'),
                    "info:modified_date": str(modifiedDate).encode('utf-8'),
                    "info:identity_id": str(identityId).encode('utf-8'),
                    "info:full_name": str(fullName).encode('utf-8'),
                    "info:username": str(username).encode('utf-8'),
                    "info:email": str(email).encode('utf-8'),
                    "info:phone": str(phone).encode('utf-8'),
                    "info:uid": str(uid).encode('utf-8'),
                    "info:client_id": str(clientId).encode('utf-8'),
                    "info:client_name": str(clientName).encode('utf-8'),
                    "info:locality_code": str(localityCode).encode('utf-8'),
                    "info:ma_tb": str(ma_tb).encode('utf-8'),
                    "info:source": str(source).encode('utf-8'),

                    "info:contract_number": str(contractNumber).encode('utf-8'),
                    "info:pricing_name": str(pricingName).encode('utf-8'),
                    "info:validity": str(validity).encode('utf-8'),
                    "info:pricing_code": str(pricingCode).encode('utf-8'),
                    "info:service_type": str(serviceType).encode('utf-8'),

                    "info:serial": str(serial).encode('utf-8'),
                    "info:valid_from": str(validFrom).encode('utf-8'),
                    "info:valid_to": str(validTo).encode('utf-8'),
                    "info:request_cert_id": str(requestCertId).encode('utf-8'),
                    "info:cert_status": str(certStatus).encode('utf-8'),
                    "info:cert_status_desc": str(certStatusDesc).encode('utf-8'),
                    f'status_log:{date_str}': str(status).encode('utf-8')
                })

            else:
                if _column_value_exists(table, row_key, "ìnfo", "status", status):
                    table.put(row_key, {'info:status': str(status).encode('utf-8')})
                    table.put(row_key, {f'status_log:{date_str}': str(status).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "status_desc", statusDesc):
                    table.put(row_key, {'info:status_desc': str(statusDesc).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "modified_date", modifiedDate):
                    table.put(row_key, {'info:modified_date': str(modifiedDate).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "serial", serial):
                    table.put(row_key, {'info:serial': str(serial).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "valid_from", validFrom):
                    table.put(row_key, {'info:valid_from': str(validFrom).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "valid_to", validTo):
                    table.put(row_key, {'info:valid_to': str(validTo).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "request_cert_id", requestCertId):
                    table.put(row_key, {'info:request_cert_id': str(requestCertId).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "cert_status", certStatus):
                    table.put(row_key, {'info:cert_status': str(certStatus).encode('utf-8')})
                if _column_value_exists(table, row_key, "ìnfo", "cert_status_desc", certStatusDesc):
                    table.put(row_key, {'info:cert_status_desc': str(certStatusDesc).encode('utf-8')})
        batch.send()
        print(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        print(f"Lỗi trong quá trình xử lý batch: {e}")
        traceback.print_exc()


def _column_value_exists(table, row_key, column_family, column_name, new_value):
    # Lấy giá trị hiện tại của cột
    current_value = table.row(row_key).get(f'{column_family}:{column_name}')
    check = current_value is not None and current_value != new_value and current_value != ''
    # So sánh giá trị hiện tại với giá trị mới
    return check
