package com.shimmer.wc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by Hui Li on 2021/3/15 21:09
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        ParameterTool pt = ParameterTool.fromArgs(args);
        int parallelism = pt.getInt("parallelism", 2);
        String host = pt.get("host");
        Integer port = pt.getInt("port");


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        DataStream<Tuple2<String, Integer>> result =
                inputDataStream
                        .flatMap(new MyFlatMapFunction())
                        .setParallelism(parallelism)
                        .keyBy(0)
                        .sum(1);

        result.print();
        env.execute();
    }
}
