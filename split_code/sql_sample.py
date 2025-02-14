import jaydebeapi
import jpype.imports

# Đường dẫn tới Phoenix JDBC JAR (thay đổi tùy theo hệ thống của bạn)
PHOENIX_JAR = "/usr/local/lakehouse/lib/phoenix-client.jar"

# Kiểm tra JVM đã khởi động chưa, nếu chưa thì khởi động
if not jpype.isJVMStarted():
    jpype.startJVM(classpath=[PHOENIX_JAR])

try:
    print("Kết nối Phoenix:")
    # Kết nối đến Phoenix
    conn = jaydebeapi.connect(
        "org.apache.phoenix.jdbc.PhoenixDriver",
        "jdbc:phoenix:localhost:2181:/hbase?phoenix.schema.isNamespaceMappingEnabled=true&phoenix.schema.mapSystemTablesToNamespace=true",
        ["", ""],
        PHOENIX_JAR
    )

    # Tạo con trỏ để thực hiện truy vấn
    cursor = conn.cursor()

    # Kiểm tra kết nối bằng cách lấy danh sách bảng
    cursor.execute("SELECT TABLE_NAME FROM SYSTEM.CATALOG WHERE TABLE_TYPE='u'")
    tables = cursor.fetchall()

    print("Danh sách bảng trong Phoenix:")
    for table in tables:
        print(table[0])

    # Đóng kết nối
    cursor.close()
    conn.close()

except Exception as e:
    print("Lỗi khi kết nối Phoenix:", e)

finally:
    # Tắt JVM nếu đã khởi động
    if jpype.isJVMStarted():
        jpype.shutdownJVM()
