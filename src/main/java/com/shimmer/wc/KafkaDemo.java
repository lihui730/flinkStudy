package com.shimmer.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import  org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.io.InvalidObjectException;
import java.util.Properties;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
/**
 * Created by Hui Li on 2021/3/18 14:19
 */
public class KafkaDemo {
    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String topic = parameterTool.get("topic");

        String kafkaGroup = parameterTool.get("group", "test");

        //初始化流式处理环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //初始化流式配置
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));

        env.enableCheckpointing(500);      //启动检查点

        //kafka配置信息
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.26.27:9092");

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroup);

        //创建flink kafka消费者，不用版本功能介绍

        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>(
                "test",  new SimpleStringSchema(), properties);

        DataStream<Tuple3<String,String,Integer>> sourceStream =
                env.addSource(myConsumer).flatMap(new myFlatMap());

        sourceStream.print();
        //注册临时表
        tableEnv.registerDataStream("tb_tmp1", sourceStream, "name,alias,id");
        //写sql语句
        String sql = "SELECT name,alias,id FROM tb_tmp1";
        //执行sql查询
        Table table = tableEnv.sqlQuery(sql);


        //打印查看数据
        DataStream<Tuple2<Boolean, Row>> wcDataStream =
                tableEnv.toRetractStream(table, Row.class);
        wcDataStream.print();

        //sql中的字段的类型
        RowTypeInfo fieldTypes = new RowTypeInfo(STRING_TYPE_INFO, STRING_TYPE_INFO, LONG_TYPE_INFO);
        //将数据追加到Table中
        DataStream<Row> retractStream =
                tableEnv.toAppendStream(table, fieldTypes);

        retractStream.print();

        env.execute("flink-kafka-test");
    }


    private static class myFlatMap implements FlatMapFunction<String, Tuple3<String, String, Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple3<String, String, Integer>> collector) throws Exception {
            String[] words = s.split(",");

            if (words.length < 3){
                throw new InvalidObjectException("record is invalid, " + s);
            }

            collector.collect(new Tuple3<>(words[0], words[1], Integer.parseInt(words[2])));
        }
    }
}