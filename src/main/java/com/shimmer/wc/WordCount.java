package com.shimmer.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.util.Collector;
import org.junit.Test;

/**
 * Created by Hui Li on 2021/3/14 21:20
 */
public class WordCount {

    @Test
    public void batchTest() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String inputPath = "D:\\Git\\flinkStudy\\src\\main\\resources\\source.txt";
        DataSet<String> inputDataSource = env.readTextFile(inputPath);
        
        DataSet<Tuple2<String, Integer>> result =
                inputDataSource.flatMap(new MyFlatMapFunction())
                        .groupBy(0)
                        .sum(1);

        result.print();
    }

    @Test
    public void streamingTestSocket() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);


        DataStream<String> inputDataStream = env.socketTextStream("localhost", 8999);

        DataStream<Tuple2<String, Integer>> result =
                inputDataStream
                        .flatMap(new MyFlatMapFunction())
                        .keyBy(0)
                        .sum(1);

        result.print();

        env.execute();

    }

    @Test
    public void streamingTest() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);


        String inputPath = "D:\\Git\\flinkStudy\\src\\main\\resources\\source.txt";
        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        DataStream<Tuple2<String, Integer>> result =
                inputDataStream
                        .flatMap(new MyFlatMapFunction())
                        .keyBy(0)
                        .sum(1);

        result.print();

        env.execute();

    }


    public static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>>{
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            for (String word: words){
                collector.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
