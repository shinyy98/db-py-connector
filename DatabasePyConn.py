# DatabasePyConn.py
import cx_Oracle
import os
import pandas as pd
from typing import Optional, Dict, Any, Union

try:
    import pymysql
    PYMYSQL_AVAILABLE = True
except ImportError:
    PYMYSQL_AVAILABLE = False


class OracleDB:
    """
    灵活的Oracle数据库连接类，支持多种资讯数据库
    """
    
    # 数据库配置预设
    DB_CONFIGS = {
        'wind': {
            'host': '192.168.105.38',
            'port': 1521,
            'service_name': 'wind',
            'username': 'wind_read',
            'password': 'Wind_read_100010',
            'schema': 'winddf',
            'description': '万得数据库'
        },
        'jyzx': {
            'host': '192.168.105.67',
            'port': 1523,
            'service_name': 'zxdb',
            'username': 'zxread',
            'password': 'Zxre70_#60#d',
            'schema': 'jyzx',
            'description': '聚源数据库'
        },
        'chf_financial': {
            'host': '192.168.105.67',
            'port': 1523,
            'service_name': 'zxdb',
            'username': 'zxread',
            'password': 'Zxre70_#60#d',
            'schema': None,  # 财汇金融库不指定schema
            'description': '财汇金融库'
        },
        'chf_company': {
            'host': '192.168.105.138',
            'port': 1523,
            'service_name': 'zxdbnew',
            'username': 'zxread',
            'password': 'Zxre80_#60#d',
            'schema': None,  # 财汇企业库不指定schema
            'description': '财汇企业库'
        },
        'zyyx': {
            'host': '192.168.105.67',
            'port': 1523,
            'service_name': 'zxdb',
            'username': 'zxread',
            'password': 'Zxre70_#60#d',
            'schema': 'zyyx',
            'description': '朝阳永续盈利预测数据库'
        }
    }
    
    def __init__(self, 
                 config_key: Optional[str] = None,
                 host: Optional[str] = None,
                 port: Optional[int] = None,
                 service_name: Optional[str] = None,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 schema: Optional[str] = None,
                 description: Optional[str] = None):
        """
        初始化数据库连接
        
        参数:
        config_key: 预设配置的键名，如'wind', 'jyzx'等
        或手动提供连接参数: host, port, service_name, username, password, schema
        """
        self.conn = None
        self.cursor = None
        
        # 如果提供了config_key，使用预设配置
        if config_key and config_key in self.DB_CONFIGS:
            config = self.DB_CONFIGS[config_key]
            self.host = host or config['host']
            self.port = port or config['port']
            self.service_name = service_name or config['service_name']
            self.username = username or config['username']
            self.password = password or config['password']
            self.schema = schema or config.get('schema')
            self.description = description or config.get('description', '自定义连接')
        else:
            # 使用手动提供的参数
            self.host = host
            self.port = port
            self.service_name = service_name
            self.username = username
            self.password = password
            self.schema = schema
            self.description = description or '自定义连接'
            
        # 验证必要参数
        if not all([self.host, self.port, self.service_name, self.username, self.password]):
            raise ValueError("缺少必要的数据库连接参数")
            
        # 创建DSN
        self.dsn = cx_Oracle.makedsn(self.host, self.port, service_name=self.service_name)
    
    @classmethod
    def from_config_key(cls, config_key: str) -> 'OracleDB':
        """
        类方法：通过配置键名快速创建实例
        """
        if config_key not in cls.DB_CONFIGS:
            available_keys = list(cls.DB_CONFIGS.keys())
            raise ValueError(f"无效的配置键名 '{config_key}'。可用键名: {available_keys}")
        
        return cls(config_key=config_key)
    
    @classmethod
    def from_custom_config(cls, 
                          host: str, 
                          port: int, 
                          service_name: str, 
                          username: str, 
                          password: str, 
                          schema: Optional[str] = None,
                          description: Optional[str] = None) -> 'OracleDB':
        """
        类方法：通过自定义配置创建实例
        """
        return cls(
            host=host,
            port=port,
            service_name=service_name,
            username=username,
            password=password,
            schema=schema,
            description=description
        )
    
    def connect(self) -> 'OracleDB':
        """
        连接到数据库
        
        返回:
        self: 支持链式调用
        """
        try:
            self.conn = cx_Oracle.connect(
                user=self.username,
                password=self.password,
                dsn=self.dsn
            )
            self.cursor = self.conn.cursor()
            
            # 如果指定了schema，设置当前schema
            if self.schema:
                self._set_current_schema(self.schema)
                
            print(f"成功连接到数据库: {self.description}")
            return self
            
        except cx_Oracle.DatabaseError as e:
            print(f"数据库连接失败 [{self.description}]: {e}")
            raise
    
    def _set_current_schema(self, schema: str) -> None:
        """
        设置当前会话的schema
        """
        try:
            sql = f"ALTER SESSION SET CURRENT_SCHEMA = {schema}"
            self.cursor.execute(sql)
        except cx_Oracle.DatabaseError as e:
            print(f"设置schema失败: {e}")
    
    def disconnect(self) -> None:
        """
        断开数据库连接
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print(f"已断开数据库连接: {self.description}")
    
    def __enter__(self):
        """
        支持上下文管理器
        """
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        退出上下文时自动断开连接
        """
        self.disconnect()
    
    def query(self, sql: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        执行SQL查询并返回DataFrame
        
        参数:
        sql: SQL查询语句
        params: 查询参数
        
        返回:
        pd.DataFrame: 查询结果
        """
        try:
            if not self.conn:
                self.connect()
            
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
            
            # 获取结果
            result = self.cursor.fetchall()
            
            # 获取列名
            if self.cursor.description:
                columns = [desc[0] for desc in self.cursor.description]
                df_result = pd.DataFrame(result, columns=columns)
            else:
                df_result = pd.DataFrame()
            
            print(f"查询成功，返回 {len(df_result)} 行数据")
            return df_result
            
        except cx_Oracle.DatabaseError as e:
            print(f"查询执行失败: {e}")
            raise
    
    def call_function(self, func_name: str, return_type: Any, *args) -> Any:
        """
        调用数据库函数
        
        参数:
        func_name: 函数名
        return_type: 返回值类型
        args: 函数参数
        
        返回:
        函数返回值
        """
        try:
            if not self.conn:
                self.connect()
            
            result = self.cursor.callfunc(func_name, return_type, args)
            return result
            
        except cx_Oracle.DatabaseError as e:
            print(f"函数调用失败: {e}")
            raise
    
    def call_procedure(self, proc_name: str, *args) -> pd.DataFrame:
        """
        调用存储过程
        
        参数:
        proc_name: 存储过程名
        args: 存储过程参数
        
        返回:
        pd.DataFrame: 存储过程返回的结果集
        """
        try:
            if not self.conn:
                self.connect()
            
            out_cursor = self.conn.cursor()
            args_list = list(args)
            args_list.insert(0, out_cursor)
            
            self.cursor.callproc(proc_name, args_list)
            
            # 获取结果
            result = out_cursor.fetchall()
            
            if out_cursor.description:
                columns = [desc[0] for desc in out_cursor.description]
                df_result = pd.DataFrame(result, columns=columns)
            else:
                df_result = pd.DataFrame()
            
            return df_result
            
        except cx_Oracle.DatabaseError as e:
            print(f"存储过程调用失败: {e}")
            raise
    
    def execute(self, sql: str, params: Optional[Dict] = None) -> int:
        """
        执行DML语句（INSERT, UPDATE, DELETE）
        
        返回:
        int: 受影响的行数
        """
        try:
            if not self.conn:
                self.connect()
            
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
            
            self.conn.commit()
            rowcount = self.cursor.rowcount
            print(f"执行成功，影响 {rowcount} 行")
            return rowcount
            
        except cx_Oracle.DatabaseError as e:
            if self.conn:
                self.conn.rollback()
            print(f"执行失败: {e}")
            raise
    
    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """
        获取表结构信息
        """
        sql = """
        SELECT 
            column_name,
            data_type,
            data_length,
            nullable,
            data_default
        FROM all_tab_columns
        WHERE table_name = UPPER(:table_name)
        ORDER BY column_id
        """
        
        if self.schema:
            sql = f"""
            SELECT 
                column_name,
                data_type,
                data_length,
                nullable,
                data_default
            FROM all_tab_columns
            WHERE owner = UPPER(:schema)
            AND table_name = UPPER(:table_name)
            ORDER BY column_id
            """
            return self.query(sql, {'schema': self.schema, 'table_name': table_name})
        else:
            return self.query(sql, {'table_name': table_name})
    
    def get_schema_tables(self) -> pd.DataFrame:
        """
        获取当前schema下的所有表
        """
        if not self.schema:
            raise ValueError("未指定schema，无法获取表列表")
        
        sql = """
        SELECT table_name
        FROM all_tables
        WHERE owner = UPPER(:schema)
        ORDER BY table_name
        """
        
        return self.query(sql, {'schema': self.schema})


# 便捷函数
def connect_wind() -> OracleDB:
    """连接到万得数据库"""
    return OracleDB.from_config_key('wind').connect()

def connect_jyzx() -> OracleDB:
    """连接到聚源数据库"""
    return OracleDB.from_config_key('jyzx').connect()

def connect_chf_financial() -> OracleDB:
    """连接到财汇金融库"""
    return OracleDB.from_config_key('chf_financial').connect()

def connect_chf_company() -> OracleDB:
    """连接到财汇企业库"""
    return OracleDB.from_config_key('chf_company').connect()

def connect_zyyx() -> OracleDB:
    """连接到朝阳永续盈利预测数据库"""
    return OracleDB.from_config_key('zyyx').connect()


# 示例用法
if __name__ == "__main__":
    # ==================== Oracle 数据库示例 ====================
    # 方法1: 使用配置键名
    with OracleDB.from_config_key('wind') as db:
        df = db.query("SELECT * FROM ChinaMutualFundDescription WHERE rownum <= 10")
        print(df.head())

    # 方法2: 使用便捷函数
    db = connect_wind()
    df = db.query("SELECT COUNT(*) FROM ChinaMutualFundDescription")
    print(f"表记录数: {df.iloc[0,0]}")
    db.disconnect()

    # 方法3: 自定义连接
    custom_db = OracleDB.from_custom_config(
        host='192.168.105.67',
        port=1523,
        service_name='zxdb',
        username='zxread',
        password='Zxre70_#60#d',
        schema='zyyx',
        description='朝阳永续盈利预测数据库'
    )

    with custom_db as db:
        tables = db.get_schema_tables()
        print(f"Schema中的表: {tables['TABLE_NAME'].tolist()}")

    # ==================== MySQL 数据库示例 ====================
    # 方法1: 使用预设配置
    # with MySQLDB.from_config_key('winddb') as db:
    #     df = db.query("SELECT * FROM fund_info LIMIT 10")
    #     print(df)

    # 方法2: 使用便捷函数
    # db = connect_winddb()
    # tables = db.get_tables()
    # print(f"数据库中的表: {tables['table_name'].tolist()}")
    # db.disconnect()

    # 方法3: 自定义连接
    # mysql_db = MySQLDB.from_custom_config(
    #     host='localhost',
    #     port=3306,
    #     user='root',
    #     password='8121',
    #     database='testdb',
    #     description='测试数据库'
    # )
    #
    # with mysql_db as db:
    #     # 查询数据
    #     df = db.query("SELECT * FROM users WHERE age > %s", (18,))
    #
    #     # 插入数据
    #     db.execute("INSERT INTO users (name, age) VALUES (%s, %s)", ("张三", 25))
    #
    #     # 批量插入
    #     data = [("李四", 30), ("王五", 28), ("赵六", 35)]
    #     db.execute_many("INSERT INTO users (name, age) VALUES (%s, %s)", data)
    #
    #     # DataFrame写入
    #     df_to_insert = pd.DataFrame({'name': ['A', 'B'], 'age': [20, 22]})
    #     db.insert_dataframe(df_to_insert, 'users')
    #
    #     # 查看表结构
    #     table_info = db.get_table_info('users')
    #     print(table_info)
    #
    #     # 调用MySQL函数
    #     result = db.call_function('DATE_FORMAT', '2024-01-15', '%Y年%m月%d日')
    #     print(f"函数结果: {result}")  # 输出: 2024年01月15日
    #
    #     # 调用存储过程（返回结果集）
    #     df = db.call_procedure('sp_get_users_by_status', 'active')
    #
    #     # 调用存储过程（有OUT参数）
    #     out_values = db.call_procedure('sp_calc_sum', 100, 200, 0)  # 第三个参数是OUT参数


class MySQLDB:
    """
    MySQL数据库连接类，支持灵活配置和多种数据库操作
    """

    # 数据库配置预设
    DB_CONFIGS = {
        'winddb': {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': '8121',
            'database': 'WINDdb',
            'charset': 'utf8mb4',
            'description': '本地Wind数据库'
        },
        'default': {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': '',
            'database': None,
            'charset': 'utf8mb4',
            'description': '默认本地MySQL'
        }
    }

    def __init__(self,
                 config_key: Optional[str] = None,
                 host: Optional[str] = None,
                 port: Optional[int] = None,
                 user: Optional[str] = None,
                 password: Optional[str] = None,
                 database: Optional[str] = None,
                 charset: str = 'utf8mb4',
                 description: Optional[str] = None):
        """
        初始化MySQL数据库连接

        参数:
        config_key: 预设配置的键名，如'winddb'等
        或手动提供连接参数: host, port, user, password, database, charset
        """
        if not PYMYSQL_AVAILABLE:
            raise ImportError("请安装 pymysql: pip install pymysql")

        self.conn = None
        self.cursor = None

        # 如果提供了config_key，使用预设配置
        if config_key and config_key in self.DB_CONFIGS:
            config = self.DB_CONFIGS[config_key]
            self.host = host or config['host']
            self.port = port or config['port']
            self.user = user or config['user']
            self.password = password or config['password']
            self.database = database or config.get('database')
            self.charset = charset or config.get('charset', 'utf8mb4')
            self.description = description or config.get('description', '自定义连接')
        else:
            # 使用手动提供的参数
            self.host = host or 'localhost'
            self.port = port or 3306
            self.user = user or 'root'
            self.password = password or ''
            self.database = database
            self.charset = charset
            self.description = description or '自定义连接'

        # 验证必要参数
        if not all([self.host, self.port, self.user]):
            raise ValueError("缺少必要的数据库连接参数(host, port, user)")

    @classmethod
    def from_config_key(cls, config_key: str) -> 'MySQLDB':
        """
        类方法：通过配置键名快速创建实例
        """
        if config_key not in cls.DB_CONFIGS:
            available_keys = list(cls.DB_CONFIGS.keys())
            raise ValueError(f"无效的配置键名 '{config_key}'。可用键名: {available_keys}")

        return cls(config_key=config_key)

    @classmethod
    def from_custom_config(cls,
                          host: str,
                          port: int,
                          user: str,
                          password: str,
                          database: Optional[str] = None,
                          charset: str = 'utf8mb4',
                          description: Optional[str] = None) -> 'MySQLDB':
        """
        类方法：通过自定义配置创建实例
        """
        return cls(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset=charset,
            description=description
        )

    def connect(self) -> 'MySQLDB':
        """
        连接到MySQL数据库

        返回:
        self: 支持链式调用
        """
        try:
            self.conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset=self.charset,
                cursorclass=pymysql.cursors.DictCursor  # 返回字典形式结果
            )
            self.cursor = self.conn.cursor()
            print(f"成功连接到MySQL数据库: {self.description}")
            return self

        except pymysql.Error as e:
            print(f"MySQL数据库连接失败 [{self.description}]: {e}")
            raise

    def disconnect(self) -> None:
        """
        断开数据库连接
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print(f"已断开MySQL数据库连接: {self.description}")

    def __enter__(self):
        """
        支持上下文管理器
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        退出上下文时自动断开连接
        """
        self.disconnect()

    def query(self, sql: str, params: Optional[Union[Dict, tuple]] = None) -> pd.DataFrame:
        """
        执行SQL查询并返回DataFrame

        参数:
        sql: SQL查询语句
        params: 查询参数（字典或元组）

        返回:
        pd.DataFrame: 查询结果
        """
        try:
            if not self.conn:
                self.connect()

            self.cursor.execute(sql, params or ())
            result = self.cursor.fetchall()

            # 转换为DataFrame
            if result:
                df_result = pd.DataFrame(result)
            else:
                df_result = pd.DataFrame()

            print(f"查询成功，返回 {len(df_result)} 行数据")
            return df_result

        except pymysql.Error as e:
            print(f"查询执行失败: {e}")
            raise

    def execute(self, sql: str, params: Optional[Union[Dict, tuple]] = None) -> int:
        """
        执行DML语句（INSERT, UPDATE, DELETE）

        返回:
        int: 受影响的行数
        """
        try:
            if not self.conn:
                self.connect()

            rowcount = self.cursor.execute(sql, params or ())
            self.conn.commit()
            print(f"执行成功，影响 {rowcount} 行")
            return rowcount

        except pymysql.Error as e:
            if self.conn:
                self.conn.rollback()
            print(f"执行失败: {e}")
            raise

    def execute_many(self, sql: str, params_list: list) -> int:
        """
        批量执行SQL语句（适用于批量插入）

        参数:
        sql: SQL语句
        params_list: 参数列表

        返回:
        int: 受影响的行数
        """
        try:
            if not self.conn:
                self.connect()

            rowcount = self.cursor.executemany(sql, params_list)
            self.conn.commit()
            print(f"批量执行成功，影响 {rowcount} 行")
            return rowcount

        except pymysql.Error as e:
            if self.conn:
                self.conn.rollback()
            print(f"批量执行失败: {e}")
            raise

    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append') -> int:
        """
        将DataFrame数据插入到MySQL表中

        参数:
        df: 要插入的DataFrame
        table_name: 目标表名
        if_exists: 'append'(追加) 或 'replace'(清空后插入)

        返回:
        int: 插入的行数
        """
        try:
            if not self.conn:
                self.connect()

            if if_exists == 'replace':
                self.execute(f"TRUNCATE TABLE {table_name}")

            # 构建INSERT语句
            columns = ', '.join([f"`{col}`" for col in df.columns])
            placeholders = ', '.join(['%s'] * len(df.columns))
            sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"

            # 转换为元组列表
            data = [tuple(row) for row in df.values]

            rowcount = self.cursor.executemany(sql, data)
            self.conn.commit()
            print(f"成功插入 {rowcount} 行数据到表 {table_name}")
            return rowcount

        except pymysql.Error as e:
            if self.conn:
                self.conn.rollback()
            print(f"DataFrame插入失败: {e}")
            raise

    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """
        获取表结构信息
        """
        sql = """
        SELECT
            COLUMN_NAME as column_name,
            DATA_TYPE as data_type,
            CHARACTER_MAXIMUM_LENGTH as max_length,
            IS_NULLABLE as is_nullable,
            COLUMN_DEFAULT as column_default,
            COLUMN_COMMENT as column_comment
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = %s
        AND TABLE_SCHEMA = DATABASE()
        ORDER BY ORDINAL_POSITION
        """
        return self.query(sql, (table_name,))

    def get_tables(self) -> pd.DataFrame:
        """
        获取当前数据库中的所有表
        """
        sql = """
        SELECT TABLE_NAME as table_name,
               TABLE_COMMENT as table_comment
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = DATABASE()
        ORDER BY TABLE_NAME
        """
        return self.query(sql)

    def ping(self, reconnect: bool = True) -> bool:
        """
        检查连接是否有效，可选重新连接

        参数:
        reconnect: 是否自动重连

        返回:
        bool: 连接是否有效
        """
        try:
            if self.conn:
                self.conn.ping(reconnect=reconnect)
                return True
            return False
        except pymysql.Error:
            return False

    def call_function(self, func_name: str, *args) -> Any:
        """
        调用MySQL函数

        参数:
        func_name: 函数名
        args: 函数参数

        返回:
        函数返回值

        示例:
            result = db.call_function('DATE_FORMAT', '2024-01-01', '%Y年%m月%d日')
        """
        try:
            if not self.conn:
                self.connect()

            # 构建参数占位符
            placeholders = ', '.join(['%s'] * len(args))
            sql = f"SELECT {func_name}({placeholders})"

            self.cursor.execute(sql, args)
            result = self.cursor.fetchone()

            if result:
                return list(result.values())[0]
            return None

        except pymysql.Error as e:
            print(f"函数调用失败: {e}")
            raise

    def call_procedure(self, proc_name: str, *args) -> Union[pd.DataFrame, list]:
        """
        调用MySQL存储过程

        参数:
        proc_name: 存储过程名
        args: 存储过程参数（支持IN/OUT/INOUT参数）

        返回:
        pd.DataFrame: 如果有结果集返回DataFrame
        list: 如果有OUT参数返回OUT参数值列表
        None: 如果既没有结果集也没有OUT参数

        示例:
            # 调用无返回的存储过程
            db.call_procedure('sp_update_status', 1, 'active')

            # 调用返回结果集的存储过程
            df = db.call_procedure('sp_get_users', 'active')

            # 调用有OUT参数的存储过程
            out_values = db.call_procedure('sp_calc_total', 100, 200, 0)  # 最后一个0是OUT参数占位符
        """
        try:
            if not self.conn:
                self.connect()

            # 调用存储过程
            result_args = self.cursor.callproc(proc_name, args)

            # 检查是否有结果集返回
            results = []
            while True:
                if self.cursor.description:
                    # 有结果集
                    data = self.cursor.fetchall()
                    if data:
                        results.append(pd.DataFrame(data))

                # 尝试获取下一个结果集
                if not self.cursor.nextset():
                    break

            # 如果有多组结果，返回列表；否则返回单个DataFrame
            if len(results) > 1:
                return results
            elif len(results) == 1:
                print(f"存储过程返回 {len(results[0])} 行数据")
                return results[0]

            # 检查是否有OUT参数返回值（与输入参数不同）
            if result_args and len(result_args) > len(args):
                out_params = result_args[len(args):]
                return list(out_params)

            # 没有结果集也没有OUT参数，返回None表示执行成功
            print(f"存储过程 '{proc_name}' 执行成功")
            return None

        except pymysql.Error as e:
            print(f"存储过程调用失败: {e}")
            raise


# MySQL便捷函数
def connect_mysql(config_key: str = 'default') -> MySQLDB:
    """通过配置键名连接到MySQL数据库"""
    return MySQLDB.from_config_key(config_key).connect()

def connect_winddb() -> MySQLDB:
    """连接到本地Wind数据库"""
    return MySQLDB.from_config_key('winddb').connect()