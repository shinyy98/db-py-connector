
### **一、 核心功能介绍**

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

### **二、 方法架构图（文字描述）**

以下以分层结构描述该类的核心架构与主要方法调用关系：

```
OracleDB 类
│
├── 【类属性/配置层】
│   └── DB_CONFIGS (字典)：存储‘wind’、‘jyzx’等预设数据库的完整连接参数。
│
├── 【构造与工厂方法层】
│   ├── __init__()：核心构造器，接受配置键或自定义参数。
│   ├── from_config_key() 【类方法】：通过配置键（如‘wind’）创建实例的工厂方法。
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