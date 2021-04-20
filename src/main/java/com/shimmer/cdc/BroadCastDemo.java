package com.shimmer.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by LH on 2021/4/14 17:52
 */
public class BroadCastDemo {
    public static void main(String[] args) throws Exception {
        //1. 流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. 执行环境配置
        env.setParallelism(1);


        //3. 配置kafka数据源DataStream
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "192.168.26.27:9092");
        kafkaProps.put("group.id", "flink_group_1");
        kafkaProps.put("session.timeout.ms", "30000");
        kafkaProps.put("auto.offset.reset", "earliest");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        FlinkKafkaConsumer<String> dataConsumer = new FlinkKafkaConsumer<String>("tb_data", new SimpleStringSchema(), kafkaProps);

        DataStreamSource<String> kafkaDataSource = env.addSource(dataConsumer);

        DataStream<Map<String, Object>> kafkaDS = kafkaDataSource.map(new MapFunction<String, Map<String, Object>>() {
            @Override
            public Map<String, Object> map(String s) throws Exception {
                return JSONObject.parseObject(s, new TypeReference<Map<String, Object>>(){});
            }
        });

        // 4.mysql配置源DataStream
        DebeziumSourceFunction<MyConfig> mysqlSource = MySQLSource.<MyConfig>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("Thinker@123")
                .databaseList("flinkdb")
                .tableList("flinkdb.tb_config")
                .debeziumProperties(new Properties())
                .deserializer(new MyDeserializationSchema())
                .build();

        MapStateDescriptor<String, MyConfig> configStateDescriptor =
                new MapStateDescriptor<>(
                        "config-cdc",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        TypeInformation.of(new TypeHint<MyConfig>() {}));

        BroadcastStream<MyConfig> configBroadCast = env.addSource(mysqlSource).broadcast(configStateDescriptor);

        DataStream<String> outDataStream = kafkaDS
                .connect(configBroadCast)
                .process(new MyBroadcastProcessFunction());

        kafkaDS.print("in");

        outDataStream.print("out");

        env.execute();
    }

    public static class MyDeserializationSchema implements DebeziumDeserializationSchema<MyConfig>{
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<MyConfig> collector) {
            Struct struct  = (Struct) sourceRecord.value(); //org.apache.kafka.connect.data.Struct
            Struct source = struct.getStruct("source");
            String db = source.getString("db");
            String table = source.getString("table");
            String op = struct.getString("op");

            if (db.equals("flinkdb") && table.equals("tb_config")){
                Struct conf = null;
                int type = 0;
                if (op.equals("c")){ //新增配置
                    conf = struct.getStruct("after");
                    type = ConfType.ADD.type;
                } else if (op.equals("u")){ //更新配置
                    conf = struct.getStruct("after");
                    type = ConfType.UPDATE.type;
                } else if (op.equals("d")){ //删除配置
                    conf = struct.getStruct("before");
                    type = ConfType.DELETE.type;
                } else return; //跳过

                int id = conf.getInt32("id");
                String tb_source = conf.getString("source");
                String parameter = conf.getString("parameter");
                int operator = conf.getInt32("operator");
                String value = conf.getString("value");
                MyConfig myConfig = new MyConfig(id, type, tb_source, parameter, operator, value);
                System.out.println("========collect config========");
                System.out.println(myConfig);

                collector.collect(myConfig);
            }
        }

        @Override
        public TypeInformation<MyConfig> getProducedType() {
            return TypeInformation.of(MyConfig.class);
        }
    }


    public static class MyBroadcastProcessFunction
            extends BroadcastProcessFunction<Map<String, Object>, MyConfig, String> {

        /**
         * configStateDescriptor用于存储广播到的配置信息
         * key是source名称，根据不同的source资源，存储一组不同的配置信息
         * value是一组配置信息列表，key是配置信息的主键，也是唯一标识；value是配置信息。map中的配置关系是与关系。
         */
        private MapStateDescriptor<String, Map<Integer, MyConfig>> configStateDescriptor =
                new MapStateDescriptor<>(
                        "config-cdc",
                        BasicTypeInfo.STRING_TYPE_INFO, //source的类型，通过source匹配data和config
                        TypeInformation.of(new TypeHint<Map<Integer, MyConfig>>() {}));

        @Override
        public void processElement(Map<String, Object> dataMap, ReadOnlyContext readOnlyContext, Collector<String> collector)
                throws Exception {
            System.out.println("======processElement=======");

            String source = dataMap.get("source").toString();

            Map<Integer, MyConfig> configMap =
                    readOnlyContext.getBroadcastState(configStateDescriptor).get(source);

            if (configMap != null && configMap.size() > 0){
                //满足配置中的所有条件，才能输出，一旦不满足，就直接返回，不采集这条数据，即过滤掉了。
                for (Map.Entry<Integer, MyConfig> configEntry: configMap.entrySet()){
                    MyConfig config = configEntry.getValue();
                    String parameter = config.parameter;
                    int operator = config.operator;
                    String value = config.value;

                    if (dataMap.containsKey(parameter)){
                        String dataValue = dataMap.get(parameter).toString();
                        if ((operator == Operator.EQUAL.operator && dataValue.equals(value))
                                || (operator == Operator.NEQUAL.operator && !dataValue.equals(value))
                                || (operator == Operator.SMALLER.operator && dataValue.compareTo(value) < 0)
                                || (operator == Operator.BIGGER.operator && dataValue.compareTo(value) > 0)
                        ) {
                            System.out.println("data: " + dataMap.toString() + " pass the inspection, conf: " + config);
                        } else {
                            System.out.println("data: " + dataMap.toString() + " didn't pass the inspection, conf: " + config);
                            return; //不满足其中一个条件，就直接返回了，不采集。
                        }
                    }
                }
            }

            collector.collect(dataMap.toString());
        }

        @Override
        public void processBroadcastElement(MyConfig myConfig, Context context, Collector<String> collector) throws Exception {
            System.out.println("======processBroadcastElement=======");

            String source = myConfig.getSource();
            int confType = myConfig.getType();
            int confID = myConfig.getId();

            BroadcastState<String, Map<Integer, MyConfig>> confState = context.getBroadcastState(configStateDescriptor);

            Map<Integer, MyConfig> confMap = confState.get(source);
            if (confMap != null && confMap.size() > 0){
                if (confType == ConfType.DELETE.type){
                    MyConfig oldConf = confMap.remove(confID); //如果配置更新的类型是删掉，那么删掉map中的配置
                    System.out.println("delete conf of source=" + source + ", id=" + confID + ", " + oldConf);
                } else {
                    confMap.put(confID, myConfig); //更新或新增配置
                    System.out.println("update or add conf of source=" + source + ", id=" + confID + ", " + myConfig);

                }
            } else if (confType != ConfType.DELETE.type){
                confMap = new HashMap<>();
                confMap.put(confID, myConfig); //更新或新增配置
                confState.put(source, confMap);
                System.out.println("update or add conf of source=" + source + ", id=" + confID + ", " + myConfig);
            }
        }
    }
    public static class MyConfig {
        private int id;            //唯一标识
        private int type;       //新增、更新、删除
        private String source;     //资源名
        private String parameter;
        private int operator;
        private String value;

        public MyConfig(int id, int type, String source, String parameter, int operator, String value) {
            this.id = id;
            this.type = type;
            this.source = source;
            this.parameter = parameter;
            this.operator = operator;
            this.value = value;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public String getParameter() {
            return parameter;
        }

        public void setParameter(String parameter) {
            this.parameter = parameter;
        }

        public int getOperator() {
            return operator;
        }

        public void setOperator(int operator) {
            this.operator = operator;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "MyConfig{" +
                    "id=" + id +
                    ", type=" + type +
                    ", source='" + source + '\'' +
                    ", parameter='" + parameter + '\'' +
                    ", operator=" + operator +
                    ", value='" + value + '\'' +
                    '}';
        }
    }

    public enum ConfType {
        ADD(0),
        UPDATE(1),
        DELETE(2);

        private int type;

        ConfType(int type) {
            this.type = type;
        }
    }

    public enum Operator {
        EQUAL(0),
        NEQUAL(1),
        SMALLER(2),
        BIGGER(3);

        private int operator;

        Operator(int op) {
            this.operator = op;
        }
    }
}
