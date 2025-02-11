import io
import json
import subprocess
import traceback
from datetime import datetime
import pytz
import happybase
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pymongo
from hdfs import InsecureClient

from Model.DataModel import Locality


def init_connect(task_file):
    try:
        with open(task_file, 'r') as f:
            task_config = json.load(f)
        hbase = task_config["hbase"]
        hbase_uri = hbase["uri"]
        hdfs_info = task_config["hdfs"]
        mongo = task_config["mongo"]
        mongo_uri = mongo["identity_uri"]
        client = InsecureClient(f'http://{hdfs_info["name_node_host"]}:{hdfs_info["port"]}', user=hdfs_info["username"], timeout=600000)
        hbase_connection = happybase.Connection(hbase_uri)
        mongo_client = pymongo.MongoClient(mongo_uri)
        tables = hbase_connection.tables()  # Liệt kê các bảng
        print(f"Kết nối thành công! {tables}")
        return hbase_connection, client, mongo_client
    except Exception as e:
        print(f"Lỗi kết nối: {e}")
        traceback.print_exc()
        raise


async def cut_off_data(task_file, table_name):
    (hbase_connection, hdfs_client, mongo_client) = init_connect(task_file)
    timezone = pytz.timezone('Asia/Bangkok')
    date_report = datetime.now(timezone)
    try:
        match table_name:
            case "Cert":
                return True
            case "CertOrder":
                return True
            case "SignatureTransaction":
                table = hbase_connection.table("SignatureTransaction")
                (start_time, end_time, count, success, file_name) = await transfer_signature_transaction(table, hdfs_client, date_report)
                new_row = {
                    "date_report": date_report.strftime('%Y%m%d%H%M%S'),
                    "start_time": start_time,
                    "end_time": end_time,
                    "table_name": table_name,
                    "file_name": file_name,
                    "description": "Cut off data by time",
                    "count": count
                }
            case "User":
                return True
            case "Credential":
                return True
            case "PersonalSignTurnOrder":
                return True
            case "Locality":
                check_success = await transfer_locality_province(hdfs_client, mongo_client, date_report)
                return check_success
            case "ProblemGuide":
                check_success = await transfer_problem_guide(hbase_connection, mongo_client)
                return check_success
            case _:
                return False
        if success:
            success = add_row_to_parquet(hdfs_client, new_row)
        if success:
            success = truncate_hbase_table(table_name, hbase_connection)
        return success

    except Exception as e:
        print(f"Lỗi khi thực hiện transfer dữ liệu bảng {table_name} lúc {datetime.now()}: {e}")
        traceback.print_exc()
    finally:
        hbase_connection.close()
        mongo_client.close()


async def transfer_signature_transaction(table, hdfs_client, date_report):
    try:
        rows = table.scan()
        data = []
        for key, value in rows:
            row = {'_id': key.decode()}
            row.update({"date_report": date_report})
            row.update({k.decode().split(":")[1]: v.decode() for k, v in value.items()})
            data.append(row)
        end_time = datetime.fromtimestamp(timestamp=(float(data[-1]['req_time']) / 1000)).strftime('%Y-%m-%d %H:%M:%S.%f')
        start_time = datetime.fromtimestamp(timestamp=(float(data[0]['req_time']) / 1000)).strftime('%Y-%m-%d %H:%M:%S.%f')
        df = pd.DataFrame(data)
        table_result = pa.Table.from_pandas(df)
        from_date = datetime.fromtimestamp(timestamp=(float(data[0]['req_time']) / 1000)).strftime('%Y%m%d%H%M%S')
        to_date = datetime.fromtimestamp(timestamp=(float(data[-1]['req_time']) / 1000)).strftime('%Y%m%d%H%M%S')
        file_name = f"signature_transaction/signature_transaction_{str(from_date)}_{str(to_date)}.parquet"
        save_file_hdfs(hdfs_client, file_name, table_result)
        return start_time, end_time, len(data), "Success", file_name
    except Exception as e:
        mess = f"Lỗi khi ghi tệp Parquet: {e}"
        print(mess)
        traceback.print_exc()
        return date_report, date_report, 0, mess, ""


def save_file_hdfs(hdfs_client, file_path, table_result):
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
                for chunk in iter(lambda: buffer_combined.read(1024 * 1024), b''):
                    final_file.write(chunk)
                    final_file.flush()
        else:
            with hdfs_client.write(file_path, overwrite=True) as hdfs_file:
                for chunk in iter(lambda: buffer.read(1024 * 1024), b''):
                    hdfs_file.write(chunk)
                    hdfs_file.flush()
        print(f"File {file_path} đã được ghi thành công lên HDFS!")
    except Exception as e:
        mess = f"Lỗi khi ghi tệp Parquet: {e}"
        print(mess)
        traceback.print_exc()
        return mess
    return ""


def add_row_to_parquet(hdfs_client, new_row):
    file_path = "config/data_cut_off.parquet"
    try:
        df = pd.DataFrame([new_row])
        table_result = pa.Table.from_pandas(df)
        save_file_hdfs(hdfs_client, file_path, table_result)
        return True
    except Exception as e:
        mess = f"Lỗi khi ghi tệp Parquet {file_path}: {e}"
        print(mess)
        traceback.print_exc()
        return False


def truncate_table(table_name):
    try:
        # Chạy lệnh truncate trong HBase shell
        command = f"echo \"truncate '{table_name}'\" | hbase shell"
        result = subprocess.run(command, shell=True, text=True, capture_output=True)

        if result.returncode == 0:
            print(f"Table '{table_name}' truncated successfully.")
            return True
        else:
            print(f"Error truncating table '{table_name}': {result.stderr}")
            return False
    except Exception as e:
        print(f"Error truncating table '{table_name}': {e}")
        traceback.print_exc()
        return False


def truncate_hbase_table(table_name, hbase_connection):
    try:
        # Disable the table
        if table_name in hbase_connection.tables():
            table_info = hbase_connection.table(table_name).families()
            hbase_connection.disable_table(table_name)

            # Delete the table
            hbase_connection.delete_table(table_name)

            # Recreate the table
            hbase_connection.create_table(table_name, table_info)
            print(f"Table '{table_name}' has been truncated.")
            return True
        else:
            print(f"Table '{table_name}' does not exist.")
            return False
    except Exception as e:
        print(f"Error truncating table '{table_name}': {e}")
        traceback.print_exc()
    return False


async def transfer_locality_province(hdfs_client, mongo_client, date_report):
    try:
        collection_ward = mongo_client["signservice_identity"]["Wards"]
        document_ward = collection_ward.find()
        data = list(document_ward)
        collection_area_local = mongo_client["signservice_identity"]["AreaLocal"]
        document_area_local = collection_area_local.find()
        data_area_local = list(document_area_local)
        data_area = {}
        file_name = f"locality/locality_{date_report.strftime('%Y%m%d%H%M%S')}.parquet"
        for row in data_area_local:
            if row.get("code") != "00":
                area_code = row.get("code", "")
                area_name = row.get("name", "")
                locality = row.get("localities", [])
                for row_ward in locality:
                    code = row_ward.get("code", "")
                    name = row_ward.get("name", "")
                    other_code = row_ward.get("otherCode", "")
                    result = {key: value for key, value in data_area.items() if (value['name'].lower() in name.lower()) or
                              (name.lower() in value['name'].lower())}
                    if result:
                        first_key = next(iter(result))
                        result[first_key]['other_code'] = code
                    else:
                        data_area[code] = {
                            "area_code": area_code,
                            "area_name": area_name,
                            "name": name,
                            "other_code": other_code
                        }

        all_data = []
        for row in data:
            province_id = row.get("ProvinceId")
            province_name = row.get("ProvinceName", "")
            province_code = row.get("ProvinceCode", "")
            district_id = row.get("DistrictId")
            district_name = row.get("DistrictName", "")
            district_id_my_vnpt = row.get("DistrictIdMyVNPT")
            ward_id = row.get("WardId")
            ward_name = row.get("WardName", "")
            ward_id_my_vnpt = row.get("WardIdMyVNPT")
            co_tuyen_thu = row.get("CoTuyenThu", False)
            item = Locality(province_id=province_id, province_name=province_name, province_code=province_code, district_id=district_id,
                            district_name=district_name, district_id_my_vnpt=district_id_my_vnpt, ward_id=ward_id, ward_name=ward_name,
                            ward_id_my_vnpt=ward_id_my_vnpt, co_tuyen_thu=co_tuyen_thu)
            province = data_area.get(province_code, {})
            if not province:
                result = {key: value for key, value in data_area.items() if (value['other_code'] == province_code)}
                if result:
                    first_key = next(iter(result))
                    province = result[first_key]
                    item.province_code = first_key
                else:
                    continue
            area_local_code = province.get("area_code", "")
            area_local_name = province.get("area_name", "")
            province_other_code = province.get("other_code", "")
            item.area_local_code = area_local_code
            item.area_local_name = area_local_name
            item.province_other_code = province_other_code
            all_data.append(item)

        # Chuyển đổi dữ liệu thành DataFrame
        values = [obj.__dict__ for obj in all_data[0:]]
        # Chuyển đổi dữ liệu thành DataFrame
        df = pd.DataFrame(values)
        grouped = df.reset_index(drop=True)
        grouped.columns = [
            'area_local_code',
            'area_local_name',
            'province_id',
            'province_name',
            'province_code',
            'province_other_code',
            'district_id',
            'district_name',
            'district_id_my_vnpt',
            'ward_id',
            'ward_name',
            'ward_id_my_vnpt',
            'co_tuyen_thu'
        ]

        schema = pa.schema([
            ('area_local_code', pa.string()),
            ('area_local_name', pa.string()),
            ('province_id', pa.int32()),
            ('province_name', pa.string()),
            ('province_code', pa.string()),
            ('province_other_code', pa.string()),
            ('district_id', pa.int32()),
            ('district_name', pa.string()),
            ('district_id_my_vnpt', pa.int32()),
            ('ward_id', pa.int32()),
            ('ward_name', pa.string()),
            ('ward_id_my_vnpt', pa.int32()),
            ('co_tuyen_thu', pa.bool_()),
        ])
        table_result = pa.Table.from_pandas(grouped, schema=schema)
        mess = save_file_hdfs(hdfs_client, file_name, table_result)
        if mess != "":
            raise mess
            return False
        new_row = {
            "date_report": date_report.strftime('%Y%m%d%H%M%S'),
            "start_time": date_report.strftime('%Y-%m-%d %H:%M:%S.%f'),
            "end_time": date_report.strftime('%Y-%m-%d %H:%M:%S.%f'),
            "table_name": "Locality",
            "file_name": file_name,
            "description": "Update data by request",
            "count": len(all_data)
        }
        success = add_row_to_parquet(hdfs_client, new_row)
        if not success:
            raise "Lỗi khi truncate bảng Province Locality"
            return False
        return True
    except Exception as e:
        print(f"Lỗi khi xử lý bảng Province Locality: {e}")
        traceback.print_exc()
        return False

    print("Chuyển dữ liệu Locality, Province, AreaLocal thành công từ MongoDB sang HBase.")


async def transfer_problem_guide(hbase_connection, mongo_client):
    try:
        collection = mongo_client["signservice_identity"]["ProblemGuide"]
        documents = collection.find()
        table = hbase_connection.table("ProblemGuide")
        for document in documents:
            row_key = str(document["_id"])
            error_message = str(document.get("ErrorMessage", "")).encode('utf-8')
            guide_content = str(document.get("GuideContent", "")).encode('utf-8')
            in_used = str(document.get("InUsed", "")).encode('utf-8')
            status = str(document.get("Status", "")).encode('utf-8')
            status_desc = str(document.get("StatusDesc", "")).encode('utf-8')
            type_error = str(document.get("type", ""))
            type_error_desc = str(document.get("type_error_desc", ""))
            created_date_check = document.get("CreatedDate", None)
            if created_date_check is not None and created_date_check != datetime(1, 1, 1, 0, 0):
                created_date = str(int(created_date_check.timestamp() * 1000))
            else:
                created_date = str(int(datetime.now().timestamp() * 1000))
            old_item = table.row(row_key)
            if old_item is None or old_item == {}:
                # Ghi vào HBase
                table.put(row_key, {
                    "info:error_message": error_message,
                    "info:guide_content": guide_content,
                    "info:in_used": in_used,
                    "info:status": status,
                    "info:status_desc": status_desc,
                    "info:type_error": type_error,
                    "info:type_error_desc": type_error_desc,
                    "info:created_date": created_date,
                })
            else:
                if column_value_exists(table, row_key, "ìnfo", "status", status):
                    table.put(row_key, {'info:status': status})
                if column_value_exists(table, row_key, "ìnfo", "status_desc", status_desc):
                    table.put(row_key, {'info:status_desc': status_desc})
                if column_value_exists(table, row_key, "ìnfo", "type_error", type_error):
                    table.put(row_key, {'info:type_error': type_error})
                if column_value_exists(table, row_key, "ìnfo", "type_error_desc", type_error_desc):
                    table.put(row_key, {'info:type_error_desc': type_error_desc})
                if column_value_exists(table, row_key, "ìnfo", "error_message", error_message):
                    table.put(row_key, {'info:error_message': error_message})
                if column_value_exists(table, row_key, "ìnfo", "guide_content", guide_content):
                    table.put(row_key, {'info:guide_content': guide_content})
    except Exception as e:
        print(f"Lỗi khi xử lý bảng Problem Guide: {e}")
        traceback.print_exc()
        return False

    print("Chuyển dữ liệu Problem Guide thành công từ MongoDB sang HBase.")
    return True


def column_value_exists(table, row_key, column_family, column_name, new_value):
    # Lấy giá trị hiện tại của cột
    current_value = table.row(row_key).get(f'{column_family}:{column_name}')
    check = current_value is not None and current_value != new_value and current_value != ''
    # So sánh giá trị hiện tại với giá trị mới
    return check
