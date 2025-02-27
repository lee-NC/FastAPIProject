import asyncio
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from pyspark.sql.functions import col, expr, when, concat_ws, year, month
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType, BooleanType, \
    ArrayType, DecimalType

from helper.get_config import init_connect_mongo, init_spark_connection

CHUNK_SIZE = 500
BATCH_SIZE = 2000
MAX_WORKERS = 4
MAX_FILE_SIZE = 536870912

logger = logging.getLogger("Lakehouse")


async def process_fetch_tables():
    current_date = datetime.now()
    end_date = current_date.replace(hour=17, minute=0, second=0) - timedelta(days=1)
    start_date = end_date - timedelta(days=1)
    logger.info(f"fetch_data at {start_date} - {end_date}")
    await load_data_identity(start_date, end_date)
    await load_data_csc(start_date, end_date)


async def load_data_identity(start_date, end_date):
    try:
        node_name = "signservice_identity"
        await load_register(node_name, start_date, end_date)
        await load_user(node_name, start_date, end_date)
        await load_cert_order(node_name, start_date, end_date)
        await load_personal_turn_order(node_name, start_date, end_date)
    except Exception as e:
        logger.error(f"Bỏ qua bảng do lỗi: {str(e)}")
        logger.error(traceback.format_exc())


async def load_data_csc(start_date, end_date):
    try:
        node_name = "signservice_credential"
        await load_credential(node_name, start_date, end_date)
        # await load_cert(node_name, start_date, end_date)
        await load_request_cert(node_name, start_date, end_date)
        await load_signature_transaction(node_name, start_date, end_date)
    except Exception as e:
        logger.error(f"Bỏ qua bảng do lỗi: {str(e)}")
        logger.error(traceback.format_exc())


async def load_cert_order(node_name, start_date, end_date):
    collection, mongo_client = init_connect_mongo(node_name, "CertOrder")
    try:
        query = [
            {
                '$match': {
                    '$or': [
                        {
                            '$and': [
                                {
                                    'CreatedDate': {
                                        '$gte': start_date
                                    }
                                }, {
                                    'CreatedDate': {
                                        '$lt': end_date
                                    }
                                }
                            ]
                        }, {
                            '$and': [
                                {
                                    'UpdatedDate': {
                                        '$gte': start_date
                                    }
                                }, {
                                    'UpdatedDate': {
                                        '$lt': end_date
                                    }
                                }
                            ]
                        }
                    ]
                }
            }, {
                '$addFields': {
                    'Logs': {
                        '$last': '$Logs'
                    },
                    'AcceptanceDocuments': {
                        '$last': '$AcceptanceDocuments'
                    }
                }
            }, {
                '$project': {
                    'full_name': '$FullName',
                    'ma_gd': '$DHSXKDCustomerInfo.ma_gd',
                    'status': '$Status',
                    'status_desc': '$StatusDesc',
                    'log_content': '$Logs.Content',
                    'log_created_date': '$Logs.CreatedDate',
                    'updated_date': '$UpdatedDate',
                    'credential_id': '$CredentialId',
                    'request_cert_id': '$RequestCertId',
                    'contract_url': '$ContractUrl',
                    'serial': '$SerialNumber',
                    'acceptance_url': '$AcceptanceDocuments.UrlSigned',
                    'identity_id': '$IdentityId',
                    'uid': '$Uid',
                    'email': '$Email',
                    'phone': '$Phone',
                    'locality_code': '$LocalityCode',
                    'type': '$Type',
                    'type_desc': '$TypeDesc',
                    'client_name': '$ClientName',
                    'client_id': '$ClientId',
                    'created_date': '$CreatedDate',
                    'ma_tb': '$DHSXKDCustomerInfo.ma_tb',
                    'ma_kh': '$DHSXKDCustomerInfo.ma_kh',
                    'ma_hd': '$DHSXKDCustomerInfo.ma_hd',
                    'ma_hrm': '$DHSXKDCustomerInfo.ma_hrm',
                    'source': '$Source',
                    'pricing_code': '$Pricing.PricingCode',
                    'pricing_name': '$Pricing.PricingName',
                    'price': '$Pricing.Price',
                    'code': '$Pricing.Code',
                    'sign_type': '$Pricing.SignType',
                    'validity': '$Pricing.TimeValidity',
                    'ma_gt': '$MaGt',
                    'previous_serial': '$PreviousSerial',
                    'province_id': '$Address.provinceId',
                    'province_name': '$Address.provinceName',
                    'district_id': '$Address.districtId',
                    'district_name': '$Address.districtName',
                    'ward_id': '$Address.wardId',
                    'ward_name': '$Address.wardName',
                    'street_name': '$Address.streetName',
                    'address': '$Address.address'
                }
            }
        ]
        documents = collection.aggregate(query).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        schema = StructType([
            StructField("_id", StringType(), True),
            StructField("acceptance_url", StringType(), True),
            StructField("address", StringType(), True),
            StructField("client_id", StringType(), True),
            StructField("client_name", StringType(), True),
            StructField("contract_url", StringType(), True),
            StructField("created_date", TimestampType(), True),
            StructField("credential_id", StringType(), True),
            StructField("district_id", StringType(), True),
            StructField("district_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("full_name", StringType(), True),
            StructField("identity_id", StringType(), True),
            StructField("locality_code", StringType(), True),
            StructField("log_content", StringType(), True),
            StructField("log_created_date", TimestampType(), True),
            StructField("ma_gd", StringType(), True),
            StructField("ma_gt", StringType(), True),
            StructField("ma_hd", StringType(), True),
            StructField("ma_hrm", StringType(), True),
            StructField("ma_kh", StringType(), True),
            StructField("ma_tb", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("previous_serial", StringType(), True),
            StructField("price", LongType(), True),
            StructField("validity", IntegerType(), True),
            StructField("pricing_code", StringType(), True),
            StructField("pricing_name", StringType(), True),
            StructField("province_id", StringType(), True),
            StructField("province_name", StringType(), True),
            StructField("request_cert_id", StringType(), True),
            StructField("sign_type", StringType(), True),
            StructField("source", IntegerType(), True),
            StructField("status", IntegerType(), True),
            StructField("status_desc", StringType(), True),
            StructField("street_name", StringType(), True),
            StructField("type", IntegerType(), True),
            StructField("type_desc", StringType(), True),
            StructField("uid", StringType(), True),
            StructField("updated_date", TimestampType(), True),
            StructField("ward_id", StringType(), True),
            StructField("ward_name", StringType(), True),
            StructField("serial", StringType(), True)
        ])
        await _process_chunks(schema, "CertOrder", documents)
    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng CertOrder: {str(e)} ")
        logger.error(traceback.format_exc())
    finally:
        mongo_client.close()
    logger.info("Chuyển dữ liệu CertOrder thành công từ MongoDB sang iceberg.")


async def _transfer_cert_order(chunk, schema):
    spark = init_spark_connection()
    try:
        df = spark.createDataFrame(chunk, schema)
        df_transformed = df.select(
            col("_id").cast(StringType()).alias("id"),
            col("identity_id").alias("identity_id"),
            col("uid").alias("uid"),
            col("full_name").alias("full_name"),
            col("email").alias("email"),
            col("phone").alias("phone"),
            col("locality_code").alias("locality_code"),
            col("type").cast(IntegerType()).alias("type"),
            col("type_desc").alias("type_desc"),
            col("status").cast(IntegerType()).alias("status"),
            col("status_desc").alias("status_desc"),
            col("client_name").alias("client_name"),
            col("client_id").alias("client_id"),
            col("created_date").cast("timestamp").alias("created_date"),
            col("updated_date").cast("timestamp").alias("modified_date"),
            col("ma_tb").alias("ma_tb"),
            col("ma_gd").alias("ma_gd"),
            col("ma_kh").alias("ma_kh"),
            col("ma_hd").alias("ma_hd"),
            col("ma_hrm").alias("ma_hrm"),
            col("source").cast(IntegerType()).alias("source"),
            col("request_cert_id").alias("request_cert_id"),
            col("serial").alias("serial"),
            col("credential_id").alias("credential_id"),
            col("pricing_code").alias("pricing_code"),
            col("pricing_name").alias("pricing_name"),
            col("price").alias("price"),
            col("sign_type").cast(IntegerType()).alias("sign_type"),
            col("validity").cast(IntegerType()).alias("validity"),
            col("ma_gt").alias("ma_gt"),
            col("previous_serial").alias("previous_serial"),
            col("contract_url").alias("contract_url"),
            col("acceptance_url").alias("acceptance_url"),
            col("log_content").alias("log_content"),
            col("log_created_date").cast("timestamp").alias("log_created_date"),
            col("province_id").cast(IntegerType()).alias("province_id"),
            col("province_name").alias("province_name"),
            col("district_id").cast(IntegerType()).alias("district_id"),
            col("district_name").alias("district_name"),
            col("ward_id").cast(IntegerType()).alias("ward_id"),
            col("ward_name").alias("ward_name"),
            col("street_name").alias("street_name"),
            col("address").alias("address_detail")
        )
        df_transformed = (df_transformed.withColumn("year_created", year(col("created_date")))
                          .withColumn("month_created", month(col("created_date"))))
        df_transformed.createOrReplaceTempView("new_data")
        spark.sql("""
                MERGE INTO iceberg.lakehouse.cert_order AS target
            USING (SELECT * FROM new_data) AS source
            ON target.id = source.id
            WHEN MATCHED THEN 
                    UPDATE SET 
                    target.status = source.status,
                    target.modified_date = source.modified_date,
                    target.status_desc = source.status_desc,
                    target.credential_id = source.credential_id,
                    target.request_cert_id = source.request_cert_id,
                    target.serial = source.serial,
                    target.contract_url = source.contract_url,
                    target.acceptance_url = source.acceptance_url,
                    target.log_content = source.log_content,
                    target.log_created_date = source.log_created_date
            WHEN NOT MATCHED THEN 
                INSERT *
            """)
        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        spark.stop()


async def load_user(node_name, start_date, end_date):
    collection_user, mongo_client = init_connect_mongo(node_name, "User")
    try:
        query = [
            {
                '$match': {
                    '$or': [
                        {
                            '$and': [
                                {
                                    'createDate': {
                                        '$gte': start_date
                                    }
                                }, {
                                    'createDate': {
                                        '$lt': end_date
                                    }
                                }
                            ]
                        }, {
                            '$and': [
                                {
                                    'modifiedDate': {
                                        '$gte': start_date
                                    }
                                }, {
                                    'modifiedDate': {
                                        '$lt': end_date
                                    }
                                }
                            ]
                        }
                    ]
                }
            },
            {
                '$project': {
                    'uid': '$uid',
                    'username': '$username',
                    'full_name': '$fullName',
                    'phone': '$phone',
                    'email': '$email',
                    'locality_code': '$localityId',
                    'uid_prefix': '$uidPrefix',
                    'user_group_id': '$userGroupId',
                    'status': '$status',
                    'status_desc': '$statusDesc',
                    'created_date': '$createDate',
                    'modified_date': '$modifiedDate',
                    'account_type': '$accountType',
                    'account_type_desc': '$acccountTypeDesc',
                    'is_com_admin': '$isComAdmin',
                    'is_test': '$isTest',
                    'pre_status': '$preStatus',
                    'province_code': '$provinceCode',
                    'province_codes': '$provinceCodes',
                    'source': '$source',
                    'roles': '$roles.name',
                    'province_id': '$identityAddress.provinceId',
                    'province_name': '$identityAddress.provinceName',
                    'district_id': '$identityAddress.districtId',
                    'district_name': '$identityAddress.districtName',
                    'ward_id': '$identityAddress.wardId',
                    'ward_name': '$identityAddress.wardName',
                    'street_name': '$identityAddress.streetName',
                    'address': '$identityAddress.address'
                }
            }
        ]
        documents_user = collection_user.aggregate(query).batch_size(CHUNK_SIZE)
        schema = StructType([
            StructField("_id", StringType(), True),
            StructField("uid", StringType(), True),
            StructField("username", StringType(), True),
            StructField("full_name", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("email", StringType(), True),
            StructField("locality_code", StringType(), True),
            StructField("uid_prefix", StringType(), True),
            StructField("user_group_id", StringType(), True),
            StructField("status", IntegerType(), True),
            StructField("status_desc", StringType(), True),
            StructField("created_date", TimestampType(), True),
            StructField("modified_date", TimestampType(), True),
            StructField("account_type", IntegerType(), True),
            StructField("account_type_desc", StringType(), True),
            StructField("is_com_admin", BooleanType(), True),
            StructField("is_test", BooleanType(), True),
            StructField("pre_status", IntegerType(), True),
            StructField("province_code", StringType(), True),
            StructField("province_codes", ArrayType(StringType()), True),
            StructField("source", IntegerType(), True),
            StructField("roles", ArrayType(StringType()), True),
            StructField("province_id", StringType(), True),
            StructField("province_name", StringType(), True),
            StructField("district_id", StringType(), True),
            StructField("district_name", StringType(), True),
            StructField("ward_id", StringType(), True),
            StructField("ward_name", StringType(), True),
            StructField("street_name", StringType(), True),
            StructField("address", StringType(), True)
        ])
        await _process_chunks(schema, "User", documents_user)
    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng User: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        mongo_client.close()
    logger.info("Chuyển dữ liệu User thành công từ MongoDB sang iceberg.")


async def load_register(node_name, start_date, end_date):
    collection_register, mongo_client = init_connect_mongo(node_name, "Register")
    try:
        query = [
            {
                '$match': {
                    '$or': [
                        {
                            '$and': [
                                {
                                    'createdDate': {
                                        '$gte': start_date
                                    }
                                }, {
                                    'createdDate': {
                                        '$lt': end_date
                                    }
                                }
                            ]
                        }, {
                            '$and': [
                                {
                                    'modifiedDate': {
                                        '$gte': start_date
                                    }
                                }, {
                                    'modifiedDate': {
                                        '$lt': end_date
                                    }
                                }
                            ]
                        }
                    ]
                }
            }, {
                '$project': {
                    'status': '$status',
                    'statusDesc': '$statusDesc',
                    'modifiedDate': '$modifiedDate',
                    'provinceId': '$address.provinceId',
                    'districtId': '$address.districtId',
                    'wardId': '$address.wardId',
                    'streetName': '$address.streetName',
                    'address': '$address.address',
                    'fullName': '$fullName',
                    'email': '$email',
                    'phone': '$phone',
                    'uid': '$uid',
                    'createdDate': '$createdDate',
                    'source': '$source'
                }
            }
        ]
        documents_register = collection_register.aggregate(query).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(None, "Register", documents_register)
    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng Register: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        mongo_client.close()
    logger.info("Chuyển dữ liệu Register thành công từ MongoDB sang iceberg.")


async def load_personal_turn_order(node_name, start_date, end_date):
    collection, mongo_client = init_connect_mongo(node_name, "PersonalSignTurnOrder")
    try:
        query = [
            {
                '$match': {
                    '$or': [
                        {
                            '$and': [
                                {
                                    'CreatedDate': {
                                        '$gte': start_date
                                    }
                                }, {
                                    'CreatedDate': {
                                        '$lt': end_date
                                    }
                                }
                            ]
                        }, {
                            '$and': [
                                {
                                    'UpdatedDate': {
                                        '$gte': start_date
                                    }
                                }, {
                                    'UpdatedDate': {
                                        '$lt': end_date
                                    }
                                }
                            ]
                        }
                    ]
                }
            }, {
                '$addFields': {
                    'Pricings': {
                        '$last': '$Pricings'
                    }
                }
            }, {
                '$project': {
                    'identity_id': '$UserInfo._id',
                    'uid': '$UserInfo.Uid',
                    'full_name': '$UserInfo.FullName',
                    'locality_code': '$UserInfo.LocalityCode',
                    'status': '$Status',
                    'status_desc': '$StatusDesc',
                    'created_date': '$CreatedDate',
                    'modified_date': '$UpdatedDate',
                    'ma_tb': '$DHSXKDCustomerInfo.ma_tb',
                    'ma_gd': '$DHSXKDCustomerInfo.ma_gd',
                    'ma_kh': '$DHSXKDCustomerInfo.ma_kh',
                    'ma_hd': '$DHSXKDCustomerInfo.ma_hd',
                    'ma_hrm': '$DHSXKDCustomerInfo.ma_hrm',
                    'credential_id': '$CredentialId',
                    'payment_order_id': '$PaymentOrderId',
                    'payment_status': '$PaymentStatus',
                    'payment_status_desc': '$PaymentStatuDesc',
                    'is_sync_dhsxkd': '$IsSyncDHSXKD',
                    'ma_gt': '$MaGt',
                    'pricing_name': '$Pricings.Name',
                    'code': '$Pricings.Code',
                    'pricing_code': '$Pricings.tocdo_id',
                    'price': '$Pricings.Price',
                    'sign_turn_number': '$Pricings.SignTurnNumber',
                    'total_money': '$TotalMoney'
                }
            }
        ]
        documents = collection.aggregate(query).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(None, "PersonalSignTurnOrder", documents)
    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng PersonalSignTurnOrder: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        mongo_client.close()
    logger.info("Chuyển dữ liệu PersonalSignTurnOrder thành công từ MongoDB sang iceberg.")


async def load_credential(node_name, start_date, end_date):
    collection, mongo_client = init_connect_mongo(node_name, "Credential")
    try:
        query = [
            {
                '$match': {
                    '$or': [
                        {
                            '$and': [
                                {
                                    'createdDate': {
                                        '$gte': start_date
                                    }
                                }, {
                                    'createdDate': {
                                        '$lt': end_date
                                    }
                                }
                            ]
                        }, {
                            '$and': [
                                {
                                    'modifiedDate': {
                                        '$gte': start_date
                                    }
                                }, {
                                    'modifiedDate': {
                                        '$lt': end_date
                                    }
                                }
                            ]
                        }
                    ]
                }
            }, {
                '$addFields': {
                    'certs': {
                        '$last': '$certs'
                    }
                }
            }, {
                '$project': {
                    'fullName': '$identity.name',
                    'subjectDN': '$subjectDN',
                    'status': '$status',
                    'statusDesc': '$statusDesc',
                    'modifiedDate': '$modifiedDate',
                    'certStatus': '$certs.status',
                    'certStatusDesc': '$certs.statusDesc',
                    'validFrom': '$certs.validFrom',
                    'validTo': '$certs.validTo',
                    'requestCertId': '$certs.requestId',
                    'serial': '$certs.serial',
                    'createdDate': '$createdDate',
                    'identityId': '$identity._id',
                    'email': '$identity.email',
                    'phone': '$identity.phone',
                    'username': '$identity.username',
                    'uid': '$identity.uid',
                    'clientId': '$identity.createdByClientId',
                    'clientName': '$identity.createdByClientName',
                    'localityCode': '$identity.LocalityCode',
                    'ma_tb': '$identity.DHSXKDSubcriptionCode',
                    'source': '$identity.source',
                    'contractNumber': '$contract.number',
                    'validity': '$contract.validity',
                    'pricingCode': '$contract.pricingCode',
                    'serviceType': '$contract.serviceType',
                    'pricingName': '$contract.servicePack'
                }
            }
        ]
        documents = collection.aggregate(query).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(None, "Credential", documents)

    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng Credential: {str(e)} ")
        logger.error(traceback.format_exc())
    finally:
        mongo_client.close()
    logger.info("Chuyển dữ liệu Credential thành công từ MongoDB sang iceberg.")


async def load_request_cert(node_name, start_date, end_date):
    collection_request_cert, mongo_client = init_connect_mongo(node_name, "RequestCert")
    try:
        query = [
            {
                '$match': {
                    '$or': [
                        {
                            '$and': [
                                {
                                    'createdDate': {
                                        '$gte': start_date
                                    }
                                }, {
                                    'createdDate': {
                                        '$lt': end_date
                                    }
                                }
                            ]
                        }, {
                            '$and': [
                                {
                                    'updatedTime': {
                                        '$gte': start_date
                                    }
                                }, {
                                    'updatedTime': {
                                        '$lt': end_date
                                    }
                                }
                            ]
                        }
                    ]
                }
            }, {
                '$addFields': {
                    'ActionLogs': {
                        '$last': '$ActionLogs'
                    }
                }
            }, {
                '$project': {
                    'status': '$status',
                    'statusDesc': '$statusDesc',
                    'updatedTime': '$updatedTime',
                    'approveTime': '$approveTime',
                    'credentialId': '$credentialId',
                    'clientId': '$createdByClientId',
                    'clientName': '$createdByClientName',
                    'createdDate': '$createdDate',
                    'pricingCode': '$pricingCode',
                    'pricingName': '$pricingName',
                    'username': '$username',
                    'localityCode': '$localityCode',
                    'uid': '$uid',
                    'identityId': '$identityId',
                    'fullName': '$fullName',
                    'period': '$period',
                    'code': '$code',
                    'requestType': '$requestType',
                    'requestTypeDesc': '$requestTypeDesc',
                    'log_content': '$ActionLogs.message',
                    'log_action': '$ActionLogs.action',
                    'log_time': '$ActionLogs.time'
                }
            }
        ]
        documents_request_cert = collection_request_cert.aggregate(query).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(None, "RequestCert", documents_request_cert)
    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng RequestCert: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        mongo_client.close()
    logger.info("Chuyển dữ liệu RequestCert thành công từ MongoDB sang iceberg.")


async def load_cert(node_name, start_date, end_date):
    collection_cert, mongo_client = init_connect_mongo(node_name, "Cert")
    try:
        query_cert = [
            {
                '$match': {
                    '$and': [
                        {
                            'validFrom': {
                                '$gte': start_date
                            }
                        }, {
                            'validFrom': {
                                '$lt': end_date
                            }
                        }
                    ]
                }
            }, {
                '$project': {
                    '_id': '$requestId',
                    'serial': '$serial',
                    'status': '$status',
                    'statusDesc': '$statusDesc',
                    'subject': '$subject',
                    'validFrom': '$validFrom',
                    'validTo': '$validTo',
                    'createdDate': '$createDate'
                }
            }
        ]
        documents_cert = collection_cert.aggregate(query_cert).batch_size(CHUNK_SIZE)
        # Xử lý song song bất đồng bộ
        await _process_chunks(None, "Cert", documents_cert)
    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng Cert: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        mongo_client.close()
    logger.info("Chuyển dữ liệu Cert thành công từ MongoDB sang iceberg.")


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
            collection_name = str(document.get("collectionName", ""))
            all_data.append(collection_name)
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
            documents = collection.find({'$and': [
                {'expiredTime': {'$gte': start_date}},
                {'expiredTime': {'$lt': end_date}}]},
                no_cursor_timeout=True).batch_size(CHUNK_SIZE)
            schema = StructType([
                StructField("_id", StringType(), True),
                StructField("credentialId", StringType(), True),
                StructField("certSerial", StringType(), True),
                StructField("identityId", StringType(), True),
                StructField("identityUid", StringType(), True),
                StructField("identityName", StringType(), True),
                StructField("identityEmail", StringType(), True),
                StructField("status", IntegerType(), True),
                StructField("statusDesc", StringType(), True),
                StructField("reqTime", TimestampType(), True),
                StructField("expiredTime", TimestampType(), True),
                StructField("finishDate", TimestampType(), True),
                StructField("tranTypeDesc", StringType(), True),
                StructField("tranType", IntegerType(), True),
                StructField("executeTime", DecimalType(10, 4), True),
                StructField("appId", StringType(), True),
                StructField("appName", StringType(), True),
                StructField("tranCode", StringType(), True)
            ])
            # Xử lý song song bất đồng bộ
            await _process_chunks(schema, "SignatureTransaction", documents)
            mongo_client.close()

    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng SignatureTransaction: {str(e)}")
        logger.error(traceback.format_exc())


async def _process_chunks(schema, collection_name, documents):
    try:
        data = list(documents)
        documents_iter = [data[i:i + BATCH_SIZE] for i in range(0, len(data), BATCH_SIZE)]

        with ThreadPoolExecutor(max_workers=MAX_WORKERS):
            tasks = [
                await asyncio.to_thread(_transfer_chunk_sync, chunk, schema, collection_name)
                for chunk in documents_iter
            ]
            if tasks:
                await asyncio.gather(*tasks)

        logger.info(f"Chuyển {len(data)} bản ghi đến bảng {collection_name} thành công từ MongoDB sang HBase.")
    except Exception as e:
        logger.error(f"Lỗi khi xử lý bảng {collection_name}: {str(e)}")
        logger.error(traceback.format_exc())


async def _transfer_chunk_sync(chunk, schema, collection_name):
    try:
        match collection_name:
            case "User":
                await _transfer_user(chunk, schema)
            case "Register":
                await _transfer_register(chunk)
            case "CertOrder":
                await _transfer_cert_order(chunk, schema)
            case "PersonalSignTurnOrder":
                await _transfer_personal_sign_turn_order(chunk)
            case "Cert":
                await _transfer_cert(chunk)
            case "RequestCert":
                await _transfer_request_cert(chunk)
            case "Credential":
                await _transfer_credential(chunk)
            case "SignatureTransaction":
                await _transfer_signature_transaction(chunk, schema)
            case _:
                logger.info(f"Bảng {collection_name} không được nhận diện.")
                return None
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý chunk: {str(e)}")
        logger.error(traceback.format_exc())


async def _transfer_signature_transaction(chunk, schema):
    spark = init_spark_connection()
    try:
        df = spark.createDataFrame(chunk, schema)
        # Select và cast các cột
        df_transformed = df.select(
            col("_id").alias("id"),
            col("certSerial").alias("serial"),
            col("credentialId").alias("credential_id"),
            col("identityId").alias("identity_id"),
            col("identityUid").alias("uid"),
            col("identityName").alias("full_name"),
            col("identityEmail").alias("email"),
            col("status").cast(IntegerType()).alias("status"),
            col("statusDesc").alias("status_desc"),
            col("reqTime").cast("timestamp").alias("req_time"),
            col("expiredTime").cast("timestamp").alias("expired_time"),
            col("finishDate").cast("timestamp").alias("finish_date"),
            col("tranTypeDesc").alias("trans_type_desc"),
            col("tranType").cast(IntegerType()).alias("trans_type"),
            col("executeTime").cast(DecimalType(10, 4)).alias("execute_time"),
            col("appId").alias("app_id"),
            col("appName").alias("app_name"),
            col("tranCode").alias("trans_code")
        )
        df_transformed = (df_transformed.withColumn("year_created", year(col("req_time")))
                          .withColumn("month_created", month(col("req_time"))))
        table_names = "iceberg.lakehouse.signature_transaction"
        (df_transformed.write.format("iceberg")
         .mode("append")
         .partitionBy('status', 'app_id', 'year_created', 'trans_type', 'month_created')
         .option("target-file-size-bytes", MAX_FILE_SIZE)
         .saveAsTable(table_names))

        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        spark.stop()


async def _transfer_register(chunk):
    spark = init_spark_connection()
    try:
        df = spark.createDataFrame(chunk)

        # Select và cast các cột
        df_transformed = df.select(
            col("_id").alias("id"),
            col("fullName").alias("full_name"),
            col("uid").alias("uid"),
            col("phone").alias("phone"),
            col("email").alias("email"),
            col("status").cast(IntegerType()).alias("status"),
            col("statusDesc").alias("status_desc"),
            col("createdDate").cast("timestamp").alias("created_date"),
            col("modifiedDate").cast("timestamp").alias("modified_date"),
            col("source").cast(IntegerType()).alias("source"),
            when(col("provinceId").isNotNull(), col("provinceId").cast(IntegerType())).otherwise(0).alias(
                "province_id"),
            when(col("districtId").isNotNull(), col("districtId").cast(IntegerType())).otherwise(0).alias(
                "district_id"),
            when(col("districtId").isNotNull(), col("districtId").cast(IntegerType())).otherwise(0).alias("ward_id"),
            col("streetName").alias("street_name"),
            col("address").alias("address")
        )
        df_transformed = (df_transformed.withColumn("year_created", year(col("created_date")))
                          .withColumn("month_created", month(col("created_date"))))
        df_transformed.createOrReplaceTempView("new_data")
        spark.sql("""
                MERGE INTO iceberg.lakehouse.register AS target
                USING (SELECT * FROM new_data) AS source
                ON target.id = source.id
                WHEN MATCHED THEN 
                    UPDATE SET 
                        target.status = source.status,
                        target.modified_date = source.modified_date,
                        target.status_desc = source.status_desc
                WHEN NOT MATCHED THEN 
                    INSERT *
                """)
        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        spark.stop()


async def _transfer_user(chunk, schema):
    spark = init_spark_connection()
    try:
        df = spark.createDataFrame(chunk, schema)
        df = df.withColumn(
            "roles",
            expr("coalesce(roles, array())")
        ).withColumn(
            "roles",
            concat_ws(",", "roles")
        )
        df = df.withColumn(
            "province_codes",
            expr("coalesce(province_codes, array())")
        ).withColumn(
            "province_codes",
            concat_ws(",", "province_codes")
        )
        df = (df.withColumn("province_id", when(col("province_id").rlike("^[0-9]+$"),
                                                col("province_id").cast(IntegerType())).otherwise(0))
              .withColumn("district_id",
                          when(col("district_id").rlike("^[0-9]+$"), col("district_id").cast(IntegerType())).otherwise(
                              0))
              .withColumn("ward_id",
                          when(col("ward_id").rlike("^[0-9]+$"), col("ward_id").cast(IntegerType())).otherwise(0)))

        df_transformed = df.select(
            col("_id").alias("id"),
            col("uid").alias("uid"),
            col("username").alias("username"),
            col("full_name").alias("full_name"),
            col("phone").alias("phone"),
            col("email").alias("email"),
            col("locality_code").alias("locality_code"),
            col("uid_prefix").alias("uid_prefix"),
            col("user_group_id").alias("user_group_id"),
            col("status").alias("status"),
            col("status_desc").alias("status_desc"),
            col("created_date").cast("timestamp").alias("created_date"),
            col("modified_date").cast("timestamp").alias("modified_date"),
            col("account_type").alias("account_type"),
            col("account_type_desc").alias("account_type_desc"),
            col("is_com_admin").cast("boolean").alias("is_com_admin"),
            col("is_test").cast("boolean").alias("is_test"),
            col("pre_status").alias("pre_status"),
            col("province_code").alias("province_code"),
            col("province_codes").alias("province_codes"),
            col("source").alias("source"),
            col("roles").alias("roles"),
            col("province_id").alias("province_id"),
            col("province_name").alias("province_name"),
            col("district_id").alias("district_id"),
            col("district_name").alias("district_name"),
            col("ward_id").alias("ward_id"),
            col("ward_name").alias("ward_name"),
            col("street_name").alias("street_name"),
            col("address").alias("address")
        )
        df_transformed = (df_transformed.withColumn("year_created", year(col("created_date")))
                          .withColumn("month_created", month(col("created_date"))))
        df_transformed.createOrReplaceTempView("new_data")
        spark.sql("""
            MERGE INTO iceberg.lakehouse.user_info AS target
        USING (SELECT * FROM new_data) AS source
        ON target.id = source.id
        WHEN MATCHED THEN 
            UPDATE SET 
                target.status = source.status,
                target.modified_date = source.modified_date,
                target.status_desc = source.status_desc,
                target.pre_status = source.pre_status,
                target.province_code = source.province_code,
                target.province_codes = source.province_codes,
                target.roles = source.roles,
                target.full_name = source.full_name,
                target.phone = source.phone,
                target.locality_code = source.locality_code,
                target.province_id = source.province_id,
                target.province_name = source.province_name,
                target.district_id = source.district_id,
                target.district_name = source.district_name,
                target.ward_id = source.ward_id,
                target.ward_name = source.ward_name,
                target.street_name = source.street_name,
                target.address = source.address
        WHEN NOT MATCHED THEN 
            INSERT *;
        """)
        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        spark.stop()


async def _transfer_personal_sign_turn_order(chunk):
    spark = init_spark_connection()
    try:
        df = spark.createDataFrame(chunk)
        df_transformed = df.select(
            col("_id").alias("id"),
            col("identity_id").alias("identity_id"),
            col("uid").alias("uid"),
            col("full_name").alias("full_name"),
            col("locality_code").alias("locality_code"),
            col("status").alias("status"),
            col("status_desc").alias("status_desc"),
            col("created_date").cast("timestamp").alias("created_date"),
            col("modified_date").cast("timestamp").alias("modified_date"),
            col("ma_tb").alias("ma_tb"),
            col("ma_gd").alias("ma_gd"),
            col("ma_kh").alias("ma_kh"),
            col("ma_hd").alias("ma_hd"),
            col("ma_hrm").alias("ma_hrm"),
            col("credential_id").alias("credential_id"),
            col("payment_order_id").alias("payment_order_id"),
            col("payment_status").cast("int").alias("payment_status"),
            col("payment_status_desc").alias("payment_status_desc"),
            col("is_sync_dhsxkd").cast("boolean").alias("is_sync_dhsxkd"),
            col("ma_gt").alias("ma_gt"),
            col("pricing_name").alias("pricing_name"),
            col("code").alias("code"),
            col("pricing_code").alias("pricing_code"),
            col("price").cast("bigint").alias("price"),
            col("sign_turn_number").alias("sign_turn_number"),
            col("total_money").cast("bigint").alias("total_money")

        )
        # Ghi vào Iceberg

        df_transformed = df_transformed.withColumn("year_created", year(col("created_date")))
        df_transformed.createOrReplaceTempView("new_data")
        spark.sql("""
            MERGE INTO iceberg.lakehouse.personal_sign_turn_order AS target
        USING (SELECT * FROM new_data) AS source
        ON target.id = source.id
        WHEN MATCHED THEN 
                UPDATE SET 
                target.status = source.status,
                target.modified_date = source.modified_date,
                target.status_desc = source.status_desc,
                target.payment_status_desc = source.payment_status_desc,
                target.payment_status = source.payment_status,
                target.is_sync_dhsxkd = source.is_sync_dhsxkd,
                target.pricing_name = source.pricing_name,
                target.code = source.code,
                target.pricing_code = source.pricing_code,
                target.price = source.price,
                target.sign_turn_number = source.sign_turn_number,
                target.total_money = source.total_money
        WHEN NOT MATCHED THEN 
            INSERT * 
            """)

        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        spark.stop()


async def _transfer_request_cert(chunk):
    keyword = "OPER"
    spark = init_spark_connection()
    try:
        df = spark.createDataFrame(chunk)

        # Select và cast các cột
        df_transformed = df.select(
            col("_id").alias("id"),
            col("credentialId").alias("credential_id"),
            col("status").cast(IntegerType()).alias("status"),
            col("statusDesc").alias("status_desc"),
            col("clientId").alias("client_id"),
            col("clientName").alias("client_name"),
            col("updatedTime").cast("timestamp").alias("modified_date"),
            col("createdDate").cast("timestamp").alias("created_date"),
            col("pricingCode").alias("pricing_code"),
            col("pricingName").alias("pricing_name"),
            col("username").alias("username"),
            col("uid").alias("uid"),
            col("identityId").alias("identity_id"),
            col("fullName").alias("full_name"),
            col("period").cast(IntegerType()).alias("period"),
            col("code").alias("code"),
            col("approveTime").cast("timestamp").alias("approve_time"),
            col("requestType").cast(IntegerType()).alias("type"),
            col("requestTypeDesc").alias("type_desc"),
            col("log_content").alias("log_content"),
            col("log_action").alias("log_action"),
            col("log_time").cast("timestamp").alias("log_time")
        )
        df_transformed = df_transformed.withColumn(
            "locality_code",
            when(
                (col("username").isNotNull()) & (col("username") != ""),
                when(
                    col("username").contains(keyword),
                    expr(f"substring(username, locate('{keyword}', username), 3)")
                ).otherwise(expr("substring(username, 4, 3)"))
            ).otherwise(""))
        df_transformed = (df_transformed.withColumn("year_created", year(col("created_date")))
                          .withColumn("month_created", month(col("created_date"))))
        df_transformed.createOrReplaceTempView("new_data")
        spark.sql("""
                    MERGE INTO iceberg.lakehouse.request_cert AS target
                USING (SELECT * FROM new_data) AS source
                ON target.id = source.id
                WHEN MATCHED THEN 
                        UPDATE SET 
                        target.status = source.status,
                        target.modified_date = source.modified_date,
                        target.status_desc = source.status_desc,
                        target.log_time = source.log_time,
                        target.log_action = source.log_action,
                        target.log_content = source.log_content
                WHEN NOT MATCHED THEN 
                    INSERT * 
                    """)
        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        spark.stop()


async def _transfer_cert(chunk):
    spark = init_spark_connection()
    try:
        df = spark.createDataFrame(chunk)

        # Select và cast các cột
        df_transformed = df.select(
            col("_id").alias("id"),
            col("status").cast(IntegerType()).alias("status"),
            col("statusDesc").alias("status_desc"),
            col("serial").alias("serial"),
            col("subject").alias("subject"),
            col("validFrom").cast("timestamp").alias("valid_from"),
            col("validTo").cast("timestamp").alias("valid_to"),
            col("createdDate").cast("timestamp").alias("created_date")
        )
        df_transformed = (df_transformed.withColumn("year_valid_from", year(col("valid_from")))
                          .withColumn("month_valid_from", month(col("valid_from"))))
        table_names = "iceberg.lakehouse.cert"
        df_transformed.createOrReplaceTempView("new_data")
        (df_transformed.write.format("iceberg")
         .mode("append")
         .partitionBy('year_valid_from', 'month_valid_from')
         .option("target-file-size-bytes", MAX_FILE_SIZE)
         .saveAsTable(table_names))
        logger.info(f"Batch với {len(chunk)} bản ghi đã được xử lý.")
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        spark.stop()


async def _transfer_credential(chunk):
    spark = init_spark_connection()
    try:
        keyword = "OPER"
        pricing_ps0 = {
            "Chứng thư số cá nhân trên các ứng dụng",
            "SmartCA cá nhân PS0 (Công dân)",
            "SmartCA Personal App Sign Limit Std"
        }
        df = spark.createDataFrame(chunk)
        df_transformed = df.select(
            col("_id").alias("id"),
            col("certStatus").cast(IntegerType()).alias("cert_status"),
            col("certStatusDesc").alias("cert_status_desc"),
            col("clientId").alias("client_id"),
            col("clientName").alias("client_name"),
            col("contractNumber").alias("contract_number"),
            col("createdDate").cast(TimestampType()).alias("created_date"),
            col("email").alias("email"),
            col("fullName").alias("full_name"),
            col("identityId").alias("identity_id"),
            col("ma_tb").alias("ma_tb"),
            col("modifiedDate").cast(TimestampType()).alias("modified_date"),
            col("phone").alias("phone"),
            col("requestCertId").alias("request_cert_id"),
            col("serial").alias("serial"),
            col("serviceType").alias("service_type"),
            col("source").cast(IntegerType()).alias("source"),
            col("status").cast(IntegerType()).alias("status"),
            col("statusDesc").alias("status_desc"),
            col("subjectDN").alias("subject_dn"),
            col("uid").alias("uid"),
            col("username").alias("username"),
            col("validFrom").alias("valid_from"),
            col("validity").cast(IntegerType()).alias("validity"),
            col("validTo").alias("valid_to"),
            when(
                ((col("pricingCode").isNull()) | (col("pricingCode") == "")) &
                (col("pricingName").isin(pricing_ps0)),
                "17187"
            ).otherwise(col("pricingCode")).alias("pricing_code"),
            when(
                (col("pricingCode") == "17187") &
                (col("pricingName").isin(pricing_ps0)),
                "SmartCA cá nhân PS0 (Công dân)"
            ).otherwise(col("pricingName")).alias("pricing_name"),
            when(
                (col("localityCode").isNull() | (col("localityCode") == "")) &
                (col("username").isNotNull() & (col("username") != "")),
                when(
                    col("username").contains(keyword),
                    expr(f"substring(username, locate('{keyword}', username), 3)")
                ).otherwise(expr("substring(username, 4, 3)"))
            ).otherwise(col("localityCode")).alias("locality_code")
        )
        df_transformed = (df_transformed.withColumn("year_valid_from", year(col("valid_from")))
                          .withColumn("month_valid_from", month(col("valid_from")))
                          .withColumn("year_valid_to", year(col("valid_to")))
                          .withColumn("month_valid_to", month(col("valid_to"))))
        df_transformed.createOrReplaceTempView("new_data")
        spark.sql("""
                    MERGE INTO iceberg.lakehouse.credential AS target
                USING (SELECT * FROM new_data) AS source
                ON target.id = source.id AND target.serial = source.serial
                WHEN MATCHED THEN 
                        UPDATE SET 
                        target.status = source.status,
                        target.modified_date = source.modified_date,
                        target.status_desc = source.status_desc
                WHEN NOT MATCHED THEN 
                    INSERT *
                """)

    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý batch: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        spark.stop()


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
