package io.debezium.connector.oracle;

import io.debezium.config.Configuration;
import org.devlive.connector.dameng.DamengConnection;

import java.util.function.Supplier;

/**
 * 为了兼容 flink-cdc oracle 中的代码 org.apache.flink.cdc.connectors.oracle.source.utils.OracleConnectionUtils
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2025/11/30
 */
public class OracleConnection extends DamengConnection {

    /**
     * 如果您需要获取达梦数据库的当前SCN，可以使用达梦数据库提供的其他视图或函数来实现。达梦数据库中并没有直接等同于Oracle的 CURRENT_SCN 的视图列，但是可以通过查询 V$LOG 或者使用系统函数来获取类似的信息。
     * 使用系统函数： 达梦数据库提供了 DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER() 函数来获取当前的SCN。
     */
    public static final String SHOW_CURRENT_SCN = "SELECT DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER() AS CURRENT_SCN FROM DUAL";

    public OracleConnection(Configuration config, Supplier<ClassLoader> classLoaderSupplier) {
        super(config, classLoaderSupplier);
    }
}
