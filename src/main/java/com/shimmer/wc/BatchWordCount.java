package com.shimmer.wc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;

import javax.sql.DataSource;

/**
 * Created by Hui Li on 2021/3/15 21:09
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        ParameterTool pt = ParameterTool.fromArgs(args);
        int parallelism = pt.getInt("parallelism", 2);
        String inputPath = pt.get("path");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> inputDataStream = env.readTextFile(inputPath);

        DataSet<Tuple2<String, Integer>> result =
                inputDataStream
                        .flatMap(new MyFlatMapFunction()).setParallelism(parallelism)
                        .groupBy(0)
                        .sum(1);

        result.print();
    }
}
