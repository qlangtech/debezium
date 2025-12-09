# 达梦与Oracle Debezium连接器日志挖掘配置差异分析

## 概述

本文档记录了达梦数据库和Oracle数据库在Debezium CDC连接器实现中关于日志挖掘（LogMining）配置的差异，特别是关于`LOG_MINING_ARCHIVE_LOG_ONLY_MODE`配置项的分析。

创建日期：2024-11-30

## 背景知识

### Oracle日志类型

Oracle数据库有两种主要的日志类型：

1. **重做日志（Redo Log）**
   - 在线日志，也称联机重做日志（Online Redo Log）
   - 固定大小和数量，循环使用
   - 包含最新的数据库变更
   - 实时性高，延迟低
   - 会被循环覆盖

2. **归档日志（Archive Log）**
   - 离线日志，是重做日志的历史副本
   - 永久保存，不会自动覆盖
   - 需要手动管理或策略清理
   - 有一定延迟（等待日志切换）
   - 用于数据库恢复和历史数据挖掘

### 达梦日志系统

达梦数据库同样具有归档日志和重做日志的概念，但在Debezium连接器的实现中采用了简化的处理方式。

## 配置差异对比

### Oracle连接器配置

Oracle连接器提供了`LOG_MINING_ARCHIVE_LOG_ONLY_MODE`配置项：

```java
// 位置：OracleConnectorConfig.java:296-305
public static final Field LOG_MINING_ARCHIVE_LOG_ONLY_MODE = Field.create("log.mining.archive.log.only.mode")
    .withDisplayName("Specifies whether log mining should only target archive logs or both archive and redo logs")
    .withType(Type.BOOLEAN)
    .withDefault(false)
    .withDescription("When set to `false`, the default, the connector will mine both archive log and redo logs to emit change events. " +
            "When set to `true`, the connector will only mine archive logs. There are circumstances where its advantageous to only " +
            "mine archive logs and accept latency in event emission due to frequent revolving redo logs.");
```

**配置选项：**
- `false`（默认）：同时挖掘归档日志和重做日志
- `true`：仅挖掘归档日志

### 达梦连接器配置

达梦连接器**没有提供**`LOG_MINING_ARCHIVE_LOG_ONLY_MODE`配置项，仅提供了时间范围控制：

```java
// 位置：DamengConnectorConfig.java:166-172
public static final Field LOG_MINING_ARCHIVE_LOG_HOURS = Field.create("log.mining.archive.log.hours")
    .withDisplayName("Log Mining Archive Log Hours")
    .withType(Type.LONG)
    .withDefault(0)
    .withDescription("The number of hours in the past from SYSDATE to mine archive logs. Using 0 mines all available archive logs");
```

## 实现差异分析

### 1. 日志管理视图差异

**Oracle使用多个视图：**
- `V$LOG` - 重做日志信息
- `V$LOGFILE` - 日志文件信息
- `V$ARCHIVED_LOG` - 归档日志信息
- `V$ARCHIVE_DEST_STATUS` - 归档目标状态

**达梦简化为主要视图：**
- `V$ARCH_FILE` - 统一的日志文件视图
- 通过STATUS字段区分状态（如'ACTIVE'表示活动日志）

#### 达梦日志视图的统一设计详解

达梦通过 `V$ARCH_FILE` 视图统一管理所有类型的日志文件，这是其与Oracle的重要区别：

**日志类型区分方式：**
- **重做日志（Redo Log）**：STATUS = 'ACTIVE' 的记录表示当前活动的重做日志
- **归档日志（Archive Log）**：其他STATUS值的记录表示已归档的日志

**代码实现证据：**

1. **获取当前重做日志**（LogMinerHelper.java:273）：
```java
// 方法名称明确表示获取"当前重做日志文件"，但查询的是V$ARCH_FILE
static Set<String> getCurrentRedoLogFiles(DamengConnection connection) {
    connection.query("SELECT PATH FROM V$ARCH_FILE WHERE STATUS = 'ACTIVE'", rs -> {
        // ...
    });
}
```

2. **获取当前SCN**（SqlUtils.java:132）：
```java
// 从ACTIVE状态的日志获取当前系统变更号
return "SELECT CLSN FROM V$ARCH_FILE WHERE STATUS = 'ACTIVE'";
```

3. **混合查询模式**（LogMinerStreamingChangeEventSource.java:330）：
```java
// 同时使用V$ARCH_FILE和V$ARCHIVED_LOG进行关联查询
"SELECT SEQUENCE# from V$ARCH_FILE F,V$ARCHIVED_LOG L WHERE F.PATH = L.NAME AND F.STATUS = 'ACTIVE'"
```

**这种设计的影响：**

1. **架构简化**：
   - 减少了视图数量，简化了查询逻辑
   - 统一的接口便于维护

2. **功能限制**：
   - 无法像Oracle那样精确分离归档日志和重做日志的处理逻辑
   - 这直接导致无法实现`LOG_MINING_ARCHIVE_LOG_ONLY_MODE`配置
   - 所有日志类型都会被统一处理，无法选择性地只读取某一种类型

3. **性能影响**：
   - 在需要区分处理不同日志类型的场景下，可能需要额外的过滤逻辑
   - 无法针对不同日志类型优化读取策略

### 2. LogMiner启动方式差异

**Oracle连接器：**
```java
// 支持多种策略和选项
String miningStrategy;
if (strategy.equals(LogMiningStrategy.CATALOG_IN_REDO)) {
    miningStrategy = "DBMS_LOGMNR.DICT_FROM_REDO_LOGS + DBMS_LOGMNR.DDL_DICT_TRACKING";
} else {
    miningStrategy = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG";
}
```

**达梦连接器：**
```java
// SqlUtils.java:229 - 固定策略
return "BEGIN DBMS_LOGMNR.START_LOGMNR(OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG); END;";
```

### 3. 功能成熟度

达梦连接器代码中有多个TODO注释，表明功能仍在完善中：

```java
// SqlUtils.java 注释片段
// todo handle INVALID file member (report somehow and continue to work with valid file)
// todo handle adding multiplexed files
// todo table level supplemental logging
// todo When you use the SKIP_CORRUPTION option
```

## 缺失该配置的可能原因

### 1. 开发优先级
- 达梦连接器是从社区项目移植而来（参考commit: f20f4d22f）
- 可能优先实现核心功能，高级特性待后续完善

### 2. 使用场景差异
- 达梦数据库的典型使用场景可能不需要如此细粒度的日志类型控制
- 通过时间范围控制可能已满足大部分需求

### 3. 技术架构差异
- 达梦数据库的日志管理机制与Oracle有本质差异
- 达梦使用`V$ARCH_FILE`统一管理所有日志文件，而Oracle使用多个独立视图
- 这种统一的视图设计使得无法在代码层面精确区分和控制不同日志类型的读取
- 简化的实现更适合达梦的架构特点，但也限制了高级功能的实现

### 4. 配置简化策略
- 减少配置项复杂度，降低用户使用门槛
- 通过`log.mining.archive.log.hours`提供简单的时间范围控制

## 影响和建议

### 对用户的影响

1. **无法精确控制日志类型**
   - 不能像Oracle连接器那样选择只读取归档日志
   - 在频繁日志切换场景下可能影响性能

2. **延迟控制受限**
   - 无法通过配置在实时性和稳定性之间做精确权衡

### 改进建议

1. **短期方案**
   - 使用`log.mining.archive.log.hours`配置间接控制
   - 通过调整批处理大小和休眠时间优化性能

2. **长期方案**
   - 向达梦官方或社区反馈需求
   - 考虑贡献代码实现该功能
   - 参考Oracle实现进行自定义修改

### 实现参考

如需添加该功能，可参考以下步骤：

1. 在`DamengConnectorConfig.java`中添加配置项
2. 修改`SqlUtils.java`的`startLogMinerStatement`方法支持不同策略
3. 在`LogMinerStreamingChangeEventSource.java`中实现日志类型过滤逻辑
4. 更新日志查询SQL以区分归档日志和重做日志

## 结论

达梦Debezium连接器目前采用了简化的日志挖掘实现，没有提供Oracle连接器中的`LOG_MINING_ARCHIVE_LOG_ONLY_MODE`配置。这种设计选择体现了在功能完整性和实现复杂度之间的权衡。随着达梦连接器的持续发展，预期会逐步完善这些高级特性。

## 参考文件

- `/opt/misc/debezium/debezium-connector-oracle/src/main/java/io/debezium/connector/oracle/OracleConnectorConfig.java`
- `/opt/misc/debezium/debezium-connector-dameng/src/main/java/org/devlive/connector/dameng/DamengConnectorConfig.java`
- `/opt/misc/debezium/debezium-connector-dameng/src/main/java/org/devlive/connector/dameng/logminer/SqlUtils.java`
- `/opt/misc/debezium/debezium-connector-dameng/src/main/java/org/devlive/connector/dameng/logminer/LogMinerStreamingChangeEventSource.java`

---

*本文档基于Debezium 1.9.8.Final版本分析*