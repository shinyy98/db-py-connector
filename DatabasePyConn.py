# DatabasePyConn.py
import cx_Oracle
import os
import pandas as pd
from typing import Optional, Dict, Any, Union


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