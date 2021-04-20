package com.shimmer.cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * Created by Hui Li on 2021/4/18 12:58
 */
public class MysqlCDCDemo {
    public static void main(String[] args) throws Exception {
        Properties mysqlProps = new Properties();
        mysqlProps.setProperty("scan.startup.mode", "initial");
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("Thinker@123")
                .debeziumProperties(mysqlProps)
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> mysqlData = env.addSource(mysqlSource);
        mysqlData.print("mysql");

        env.execute();
    }
}
