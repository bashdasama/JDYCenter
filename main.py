import pyodbc
import time
# 配置源数据库服务器连接信息
source_server = "10.13.112.6"
source_database = "HR"
source_user = "sa"
source_password = "ccrbnc_2016"

# 配置目标数据库服务器连接信息
destination_server = "10.13.112.13"
destination_database = "JDYCenter"
destination_user = "sa"
destination_password = "ccrbnc_2016"


def copy_data():


    # 连接源数据库服务器
    source_conn_str = f"DRIVER=SQL Server;SERVER={source_server};DATABASE={source_database};UID={source_user};PWD={source_password}"
    source_conn = pyodbc.connect(source_conn_str)
    source_cursor = source_conn.cursor()

    # 连接目标数据库服务器
    destination_conn_str = f"DRIVER=SQL Server;SERVER={destination_server};DATABASE={destination_database};UID={destination_user};PWD={destination_password}"
    destination_conn = pyodbc.connect(destination_conn_str)
    destination_cursor = destination_conn.cursor()


    try:
        # 查询源表格的数据
        source_cursor.execute("SELECT * FROM UsrA01")

        # 创建目标表格，包括列名和数据类型
        # 注意：这里的列名和数据类型需要根据实际情况修改
        destination_cursor.execute("""
                CREATE TABLE UsrA01 (
                    A0000 INT,
                    A0000 INT,
                    A0100 VARCHAR(255),
                    B0110 VARCHAR(255),
                    A0111 DATETIME,
                    A0141 DATETIME,
                    A0107 VARCHAR(255),
                    A0177 VARCHAR(255),
                    A0121 VARCHAR(255),
                    A0151 INT,
                    State VARCHAR(255),
                    C0101 INT,
                    E01A1 VARCHAR(255),
                    CreateTime DATETIME,
                    ModTime DATETIME,
                    CreateUserName VARCHAR(255),
                    ModUserName VARCHAR(255),
                    UserName VARCHAR(255),
                    UserPassword VARCHAR(255),
                    Groups VARCHAR(255),
                    C0183 DATETIME,
                    C0181 VARCHAR(255),
                    ModTime1 DATETIME,
                    A0114 VARCHAR(255),
                    A0124 VARCHAR(255),
                    A0127 VARCHAR(255),
                    E0122 VARCHAR(255),
                    C01TC VARCHAR(255),
                    C01UC VARCHAR(255),
                    A0101 VARCHAR(255),
                    C01ZY VARCHAR(255),
                    C0103 VARCHAR(255),
                    A0174 VARCHAR(255),
                    C0102 VARCHAR(255),
                    C0104 VARCHAR(255),
                    C0105 INT,
                    A01Z0 VARCHAR(255),
                    GUIDKEY VARCHAR(255),
                    H01SM VARCHAR(255),
                    H01SN DATETIME,
                    C0109 VARCHAR(255),
                    C010A VARCHAR(255),
                    C010B numeric,
                    H01SV VARCHAR(255),
                    H01SW VARCHAR(255),
                    H01SX VARCHAR(255),
                    C010F VARCHAR(255),
                    H01T5 VARCHAR(255),
                    H01T6 VARCHAR(255),
                    H01T7 VARCHAR(255),
                    H01TE VARCHAR(255),
                    H01TF VARCHAR(255),
                    H01TG VARCHAR(255),
                    H01TH VARCHAR(255),
                    H01TN DATETIME,
                    H01TP VARCHAR(255),
                    H01SY VARCHAR(255),
                    H01TM VARCHAR(255),
                    H01TQ VARCHAR(255),
                    H01TR VARCHAR(255),
                    C01ZZ VARCHAR(255),
                    H01TS DATETIME,
                    H01TT VARCHAR(255),
                    H01TU VARCHAR(255),
                    H01TA VARCHAR(255),
                    H01TV VARCHAR(255),
                    H01TX VARCHAR(255),
                    H01TW VARCHAR(255),
                    H01U1 DATETIME,
                    H01U3 VARCHAR(255),
                    H01U2 VARCHAR(255),
                    H01U5 DATETIME,
                    H01U7 VARCHAR(255),
                    H01U6 VARCHAR(255),
                    H01TZ VARCHAR(255),
                    H01U8 VARCHAR(255),
                    H01TY VARCHAR(255),
                    H01U0 VARCHAR(255),
                    H01U4 VARCHAR(255),
                    H01UA DATETIME,
                    H01U9 DATETIME,
                    H01UB VARCHAR(255),
                    H01UC VARCHAR(255),
                    C0106 VARCHAR(255),
                    C0107 VARCHAR(255),
                    H01UF DATETIME,
                    H01UG DATETIME,
                    H01UD DATETIME,
                    H01UE DATETIME,
                    H01UJ DATETIME,
                    H01UK DATETIME,
                    H01UH DATETIME,
                    H01UI DATETIME,
                    H01UL DATETIME,
                    H01UM DATETIME,
                    H01UN VARCHAR(255),
                    H01UO VARCHAR(255),
                    H01UQ DATETIME,
                    H01UP DATETIME,
                    H01UR VARCHAR(255),
                    H01US VARCHAR(255),
                    H01UT VARCHAR(255),
                    H01UU DATETIME,
                    C0108 VARCHAR(255),
                    H01UV INT,
                    H01UW INT,
                    H01UX VARCHAR(255),
                    H01UY VARCHAR(255),
                    H01UZ VARCHAR(255),
                    H01V0 INT,
                    H01V1 VARCHAR(255),
                    H01V2 VARCHAR(255),
                    H01V4 VARCHAR(255),
                    A01AA VARCHAR(255),
                    A01AB VARCHAR(255),
                    A01AC DATETIME,
                    A01AD VARCHAR(255),
                    A01AE VARCHAR(255),
                    A01AF DATETIME,
                    A01AG DATETIME,
                    A01AJ DATETIME,
                    A01AI DATETIME,
                    A01AH VARCHAR(255),
                    A01AK VARCHAR(255),
                    A01AL VARCHAR(255),
                    A01AM VARCHAR(255),
                    A01AN VARCHAR(255)
                )
            """)
        destination_conn.commit()

        # 将数据从源表格复制到目标表格
        for row in source_cursor:
            # 注意：这里的列名需要根据实际情况修改
            destination_cursor.execute("""
                    INSERT INTO UsrA01 (A0000, A0100, B0110, A0111, A0141, ...)
                    VALUES (?, ?, ?, ?, ?, ...)
                """, row)

        destination_conn.commit()
        print("表格 UsrA01 成功复制到目标数据库")
    except Exception as e:
        destination_conn.rollback()
        print(f"复制表格时发生错误：{e}")
    finally:
        source_conn.close()
        destination_conn.close()



def crate_table():
    # 建立源数据库连接
    source_connection_string = (
        f'DRIVER=SQL Server;'
        f'SERVER={source_server};'
        f'DATABASE={source_database};'
        f'UID={source_user};'
        f'PWD={source_password};'
    )
    source_connection = pyodbc.connect(source_connection_string)
    source_cursor = source_connection.cursor()

    # 建立新建数据库连接
    destination_connection_string = (
        f'DRIVER=SQL Server;'
        f'SERVER={destination_server};'
        f'DATABASE={destination_database};'
        f'UID={destination_user};'
        f'PWD={destination_password};'
    )
    destination_connection = pyodbc.connect(destination_connection_string)
    destination_cursor = destination_connection.cursor()

    # 源数据库表格名称
    source_table_name = 'usrA01'

    # 新建数据库表格名称
    destination_table_name = 'usrA01'

    try:
        # 获取源数据库表格结构
        source_cursor.execute(f"SELECT TOP 0 A0101,A0177,A0141 FROM {source_table_name}")
        table_columns = [column[0] for column in source_cursor.description]
        column_names = ', '.join(table_columns)

        # 创建新建数据库中的表格
        create_table_sql = f"CREATE TABLE {destination_table_name} ({column_names})"
        destination_cursor.execute(create_table_sql)
        destination_connection.commit()

        # 从源数据库复制数据到新建数据库
        insert_data_sql = f"INSERT INTO {destination_table_name} ({column_names}) SELECT {column_names} FROM {source_table_name}"
        destination_cursor.execute(insert_data_sql)
        destination_connection.commit()



        print(f"表格 {source_table_name} 成功复制到 {destination_table_name}")
    except Exception as e:
        print(f"复制表格时发生错误：{e}")

    finally:
        # 关闭连接
        source_cursor.close()
        source_connection.close()
        destination_cursor.close()
        destination_connection.close()


def sql_help():
    # 连接到数据库
    conn_str = f"DRIVER=SQL Server;SERVER={source_server};DATABASE={source_database};UID={source_user};PWD={source_password}"
    conn = pyodbc.connect(conn_str)

    # 使用 sp_help 系统存储过程查看表结构
    table_name = "UsrA01"
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
        rows = cursor.fetchall()
        for row in rows:
            print(row)

    # 关闭连接
    conn.close()

if __name__ == "__main__":
    sql_help()
    # crate_table()
    # copy_data()
    # 每隔一段时间执行数据复制操作
    # while True:
    #     copy_data()
    #     # 假设每隔 1 小时复制一次
    #     time.sleep(3600)