### 示例 1：基础用法 - 连接与查询（使用配置键名）

这是最常用的方式，利用类中预定义的配置快速连接指定数据库。

```python
# 导入数据库连接类
from DatabasePyConn import OracleDB

# 方法A：使用上下文管理器（推荐，自动管理连接）
print("=== 示例1A：使用 with 语句连接万得（wind）数据库 ===")
with OracleDB.from_config_key('wind') as db:
    # 执行一个简单的查询，获取前5条基金描述记录
    df = db.query("SELECT fund_id, fund_name FROM ChinaMutualFundDescription WHERE rownum <= 5")
    print(df)

# 方法B：手动管理连接生命周期
print("\n=== 示例1B：手动连接聚源（jyzx）数据库并探索 ===")
db_jy = OracleDB.from_config_key('jyzx')
try:
    db_jy.connect()
    # 获取当前schema下的所有表
    tables = db_jy.get_schema_tables()
    print(f"聚源数据库 '{db_jy.schema}' 模式下共有 {len(tables)} 张表")
    print("前10张表：", tables['TABLE_NAME'].head(10).tolist())
finally:
    db_jy.disconnect()  # 确保连接被关闭
```

### 示例 2：使用便捷函数

文档末尾提供了更快捷的函数，用于连接特定的数据库。

```python
# 从模块中导入便捷函数
from DatabasePyConn import connect_wind, connect_zyyx

print("=== 示例2：使用便捷函数 ===")
# 连接到万得数据库
db_wind = connect_wind()
# 查询记录总数
result = db_wind.query("SELECT COUNT(*) as total_funds FROM ChinaMutualFundDescription")
print(f"万得基金描述表总记录数: {result.iloc[0,0]}")
db_wind.disconnect()

# 连接到朝阳永续数据库并使用上下文管理器
with connect_zyyx() as db_zy:
    # 假设查询盈利预测表的结构
    table_info = db_zy.get_table_info('YOUR_PROFIT_TABLE')  # 请替换为实际表名
    print(f"\n表结构预览（共{len(table_info)}个字段）:")
    print(table_info[['COLUMN_NAME', 'DATA_TYPE']].head())
```

### 示例 3：自定义配置与高级操作

展示如何连接一个未预定义的数据库，并进行插入、更新等操作。

```python
from DatabasePyConn import OracleDB
import cx_Oracle  # 用于定义函数返回类型

print("=== 示例3：自定义配置与DML操作 ===")
# 1. 连接到自定义的测试数据库
custom_db = OracleDB.from_custom_config(
    host='192.168.1.100',
    port=1521,
    service_name='ORCLPDB',
    username='scott',
    password='tiger',
    schema='test_schema',
    description='自定义测试数据库'
)

with custom_db as db:
    # 2. 执行INSERT语句（假设有表和权限）
    insert_sql = """
    INSERT INTO employees (id, name, department) 
    VALUES (:id, :name, :dept)
    """
    params = {'id': 101, 'name': '张三', 'dept': '技术部'}
    # affected_rows = db.execute(insert_sql, params)  # 实际执行时取消注释
    # print(f"插入了 {affected_rows} 行数据。")
    print("(注释: 上述INSERT语句已准备好，实际执行需取消代码注释并确保表存在)")

    # 3. 执行UPDATE语句
    update_sql = "UPDATE employees SET department = :new_dept WHERE id = :emp_id"
    # db.execute(update_sql, {'new_dept': '研发部', 'emp_id': 101})

    # 4. 带参数的查询
    select_sql = "SELECT * FROM employees WHERE id = :emp_id"
    # df_emp = db.query(select_sql, {'emp_id': 101})
    # print(df_emp)

    # 5. 调用数据库函数（示例，假设存在函数 GET_EMPLOYEE_NAME）
    # try:
    #     emp_name = db.call_function('GET_EMPLOYEE_NAME', str, 101)  # str 是返回类型
    #     print(f"通过函数获取的名称: {emp_name}")
    # except Exception as e:
    #     print(f"函数调用失败: {e}")
```

### 示例 4：多数据库操作与表结构分析

模拟一个需要从多个数据库获取数据的简单分析场景。

```python
from DatabasePyConn import OracleDB
import pandas as pd

print("=== 示例4：从多个数据库获取信息 ===")
# 假设我们需要对比不同数据库的某类表结构
db_configs_to_compare = ['wind', 'jyzx']
all_columns = []

for config_key in db_configs_to_compare:
    try:
        with OracleDB.from_config_key(config_key) as db:
            # 获取表列表（这里以获取前5个表名为例）
            tables = db.get_schema_tables().head(5)
            print(f"\n数据库 '{db.description}' 的前5张表: {tables['TABLE_NAME'].tolist()}")
            
            # 获取某张特定表的结构（例如，假设都有名为 'SECURITY' 的表）
            # 注意：这是一个示例，实际表名需替换为存在的表
            # try:
            #     info = db.get_table_info('SECURITY')
            #     info['SOURCE_DB'] = db.description
            #     all_columns.append(info)
            #     print(f"  - 表 'SECURITY' 有 {len(info)} 个字段")
            # except Exception as e:
            #     print(f"  - 获取表 'SECURITY' 结构失败: {e}")
    except Exception as e:
        print(f"连接数据库 '{config_key}' 失败: {e}")

# 如果成功收集到信息，可以进行对比分析
# if all_columns:
#     combined_df = pd.concat(all_columns, ignore_index=True)
#     print(f"\n合并后的字段信息总览（共{len(combined_df)}行）:")
#     print(combined_df[['SOURCE_DB', 'COLUMN_NAME', 'DATA_TYPE']].head(10))
```

### 使用建议总结：

1. **选择连接方式**：
    
    - **已知配置**：使用 `OracleDB.from_config_key('配置键')`（如 `'wind'`）或对应的便捷函数（如 `connect_wind()`）。
        
    - **新数据库**：使用 `OracleDB.from_custom_config(...)`传入自定义参数。
        
    
2. **管理连接资源**：
    
    - **强烈推荐**使用 `with OracleDB.from_config_key(...) as db:`模式，可自动处理连接的打开和关闭。
        
    - 若手动管理，务必在 `try...finally`块中或操作结束后调用 `.disconnect()`。
        
    
3. **执行操作**：
    
    - **查询数据**：使用 `.query(sql, params)`，它返回 `pandas.DataFrame`。
        
    - **修改数据**：使用 `.execute(sql, params)`执行 INSERT/UPDATE/DELETE，它会返回受影响的行数并自动提交事务（失败时自动回滚）。
        
    - **探索结构**：使用 `.get_schema_tables()`和 `.get_table_info(table_name)`。
        
    - **调用程序**：使用 `.call_function()`和 `.call_procedure()`调用数据库中的函数和存储过程。