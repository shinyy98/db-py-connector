
# DatabasePyConn 数据库连接库

本库提供统一的 Python 数据库连接工具，支持 **Oracle** 和 **MySQL** 两种主流数据库。

---

## 目录

1. [OracleDB 类](#一-oracledb-类)
2. [MySQLDB 类](#二-mysqldb-类)
3. [快速开始](#三-快速开始)
4. [依赖安装](#四-依赖安装)

---

## **一、 OracleDB 类**

### **1.1 核心功能介绍**

`OracleDB`类是一个专为简化 Python 与多种 Oracle 数据库交互而设计的封装工具。其核心功能围绕**灵活连接**、**便捷操作**和**安全资源管理**展开。

1. **灵活的多元化连接管理**：

    - **预设连接池**：内置了对万得（Wind）、聚源（Jyzx）、财汇（Chf）、朝阳永续（Zyyx）等国内常用金融数据库的配置，通过一个键名（如 `'wind'`）即可快速连接。

    - **自定义连接**：支持通过传入完整参数（主机、端口、服务名、用户名、密码等）连接任意 Oracle 数据库。

    - **Schema 自动设置**：在连接时，可自动将会话的当前模式（Schema）切换到指定模式，简化后续查询。


2. **统一的数据操作接口**：

    - **查询与获取**：`query`方法执行 SQL 查询，并**直接返回 `pandas.DataFrame`**，无缝对接数据分析流程。

    - **数据修改**：`execute`方法执行插入、更新、删除操作，自动处理事务提交与回滚，并返回受影响的行数。

    - **程序调用**：`call_function`和 `call_procedure`方法支持调用数据库内已存储的函数和存储过程。


3. **数据库结构探索**：

    - 提供 `get_table_info`和 `get_schema_tables`方法，用于获取表结构元数据和列出当前模式下的所有表，方便用户在不熟悉数据库结构时进行探索。


4. **安全的资源生命周期管理**：

    - 支持 **Python 上下文管理器协议**（即 `with`语句），确保数据库连接在使用完毕后自动关闭，避免资源泄漏。

    - 同时支持手动调用 `connect`和 `disconnect`方法进行精细控制。


5. **开箱即用的便捷函数**：

    - 模块提供了如 `connect_wind()`, `connect_jyzx()`等一系列顶级函数，是连接特定数据库的最快捷方式。


---

### **1.2 方法架构图（文字描述）**

```
OracleDB 类
│
├── 【类属性/配置层】
│   └── DB_CONFIGS (字典)：存储'wind'、'jyzx'等预设数据库的完整连接参数。
│
├── 【构造与工厂方法层】
│   ├── __init__()：核心构造器，接受配置键或自定义参数。
│   ├── from_config_key() 【类方法】：通过配置键（如'wind'）创建实例的工厂方法。
│   └── from_custom_config() 【类方法】：通过完整自定义参数创建实例的工厂方法。
│
├── 【连接管理层】
│   ├── connect()：建立数据库连接，获取游标，并可设置当前Schema。
│   ├── _set_current_schema() 【内部方法】：执行设置当前模式的SQL。
│   ├── disconnect()：关闭游标与连接，释放资源。
│   ├── __enter__()：支持`with`语句，进入时自动连接。
│   └── __exit__()：支持`with`语句，退出时自动断开连接。
│
├── 【数据操作层】（核心）
│   ├── query(sql, params) -> pd.DataFrame
│   │   └── 执行查询，返回结果集（DataFrame格式）。
│   │
│   ├── execute(sql, params) -> int
│   │   └── 执行DML语句，返回受影响行数，自动管理事务。
│   │
│   ├── call_function(func_name, return_type, *args) -> Any
│   │   └── 调用数据库标量函数。
│   │
│   └── call_procedure(proc_name, *args) -> pd.DataFrame
│       └── 调用返回游标的存储过程，并将结果转为DataFrame。
│
├── 【元数据探索层】
│   ├── get_table_info(table_name) -> pd.DataFrame
│   │   └── 查询指定表的列信息。
│   │
│   └── get_schema_tables() -> pd.DataFrame
│       └── 列出当前Schema下的所有表。
│
└── 【模块级便捷函数】（非类方法）
    ├── connect_wind() -> OracleDB
    ├── connect_jyzx() -> OracleDB
    ├── connect_chf_financial() -> OracleDB
    ├── connect_chf_company() -> OracleDB
    └── connect_zyyx() -> OracleDB
```

**架构特点总结**：

1. **配置与实例分离**：通过工厂方法 (`from_config_key`) 创建实例，使配置管理更加清晰。
2. **资源管理自动化**：通过上下文管理器，将连接生命周期管理与业务逻辑解耦。
3. **接口统一且实用**：所有数据获取方法均返回 `pandas.DataFrame`，与数据科学生态无缝集成；`execute`方法封装了事务逻辑。
4. **功能分层明确**：从配置、连接到数据操作、元数据查询，各层职责单一，易于理解和使用。

---

## **二、 MySQLDB 类**

### **2.1 核心功能介绍**

`MySQLDB`类是专为 MySQL 数据库设计的封装工具，API 设计与 `OracleDB` 保持一致，方便用户无缝切换。

1. **灵活的连接管理**：

    - **预设配置**：内置本地数据库配置，支持通过配置键快速连接。
    - **自定义连接**：支持完整参数自定义连接任意 MySQL 数据库。
    - **自动重连**：内置 `ping()` 方法检测连接有效性并自动重连。

2. **统一的数据操作接口**：

    - **查询与获取**：`query()` 方法返回 `pandas.DataFrame`。
    - **数据修改**：`execute()` 方法执行 DML 语句，自动管理事务。
    - **批量操作**：`execute_many()` 支持批量插入。
    - **DataFrame 写入**：`insert_dataframe()` 直接将 DataFrame 写入数据库表。

3. **函数与存储过程**：

    - `call_function()`：调用 MySQL 内置或自定义函数。
    - `call_procedure()`：调用存储过程，支持 IN/OUT 参数。

4. **元数据查询**：

    - `get_tables()`：获取当前数据库的所有表。
    - `get_table_info()`：获取指定表的列信息。

5. **资源管理**：支持 `with` 上下文管理器，自动管理连接生命周期。

---

### **2.2 方法架构图（文字描述）**

```
MySQLDB 类
│
├── 【类属性/配置层】
│   └── DB_CONFIGS (字典)：存储'winddb'、'default'等预设数据库连接参数。
│
├── 【构造与工厂方法层】
│   ├── __init__()：核心构造器，接受配置键或自定义参数。
│   ├── from_config_key() 【类方法】：通过配置键创建实例。
│   └── from_custom_config() 【类方法】：通过完整参数创建实例。
│
├── 【连接管理层】
│   ├── connect()：建立数据库连接，使用 DictCursor 返回字典形式结果。
│   ├── disconnect()：关闭游标与连接。
│   ├── ping(reconnect)：检查连接有效性，可选自动重连。
│   ├── __enter__() / __exit__()：支持上下文管理器。
│
├── 【数据操作层】（核心）
│   ├── query(sql, params) -> pd.DataFrame
│   │   └── 执行查询，返回 DataFrame。
│   │
│   ├── execute(sql, params) -> int
│   │   └── 执行 INSERT/UPDATE/DELETE，返回受影响行数。
│   │
│   ├── execute_many(sql, params_list) -> int
│   │   └── 批量执行 SQL，适用于批量插入。
│   │
│   └── insert_dataframe(df, table_name, if_exists) -> int
│       └── 将 DataFrame 写入数据库表（支持 append/replace）。
│
├── 【程序调用层】
│   ├── call_function(func_name, *args) -> Any
│   │   └── 调用 MySQL 函数，如 DATE_FORMAT, CONCAT 等。
│   │
│   └── call_procedure(proc_name, *args) -> Union[pd.DataFrame, list, None]
│       └── 调用存储过程，支持返回结果集或 OUT 参数。
│
├── 【元数据探索层】
│   ├── get_table_info(table_name) -> pd.DataFrame
│   │   └── 查询 INFORMATION_SCHEMA.COLUMNS 获取表结构。
│   │
│   └── get_tables() -> pd.DataFrame
│       └── 查询 INFORMATION_SCHEMA.TABLES 获取所有表。
│
└── 【模块级便捷函数】（非类方法）
    ├── connect_mysql(config_key='default') -> MySQLDB
    └── connect_winddb() -> MySQLDB
```

---

### **2.3 使用示例**

```python
from DatabasePyConn import MySQLDB, connect_winddb
import pandas as pd

# ========== 方式1: 使用上下文管理器（推荐）==========
with MySQLDB.from_custom_config(
    host='localhost',
    port=3306,
    user='root',
    password='8121',
    database='WINDdb'
) as db:
    # 查询数据
    df = db.query("SELECT * FROM fund_info LIMIT 10")
    print(df)

    # 插入数据
    db.execute("INSERT INTO users (name, age) VALUES (%s, %s)", ("张三", 25))

    # 批量插入
    data = [("李四", 30), ("王五", 28)]
    db.execute_many("INSERT INTO users (name, age) VALUES (%s, %s)", data)

    # DataFrame 写入
    df_data = pd.DataFrame({'name': ['A', 'B'], 'age': [20, 22]})
    db.insert_dataframe(df_data, 'users')

    # 调用函数
    result = db.call_function('DATE_FORMAT', '2024-01-15', '%Y年%m月%d日')
    print(result)  # 输出: 2024年01月15日

# ========== 方式2: 使用预设配置 ==========
with MySQLDB.from_config_key('winddb') as db:
    tables = db.get_tables()
    print(tables)

# ========== 方式3: 使用便捷函数 ==========
db = connect_winddb()
df = db.query("SELECT VERSION() as version")
print(df.iloc[0]['version'])
db.disconnect()
```

---

## **三、 快速开始**

### **3.1 Oracle 数据库连接**

```python
from DatabasePyConn import OracleDB, connect_wind

# 使用预设配置连接 Wind 数据库
with OracleDB.from_config_key('wind') as db:
    df = db.query("SELECT * FROM ChinaMutualFundDescription WHERE rownum <= 10")
    print(df)

# 或使用便捷函数
db = connect_wind()
df = db.query("SELECT COUNT(*) FROM ChinaMutualFundDescription")
db.disconnect()
```

### **3.2 MySQL 数据库连接**

```python
from DatabasePyConn import MySQLDB

# 自定义配置连接本地 MySQL
with MySQLDB.from_custom_config(
    host='localhost',
    port=3306,
    user='root',
    password='your_password',
    database='your_database'
) as db:
    df = db.query("SELECT * FROM your_table LIMIT 10")
    print(df)
```

---

## **四、 依赖安装**

```bash
# Oracle 支持（cx_Oracle 需要 Oracle 客户端）
pip install cx_Oracle pandas

# MySQL 支持
pip install pymysql pandas

# 全部安装
pip install cx_Oracle pymysql pandas
```

### **注意事项**

- **Oracle**: 需要安装 [Oracle Instant Client](https://www.oracle.com/database/technologies/instant-client.html)
- **MySQL**: 纯 Python 实现，无需额外依赖

