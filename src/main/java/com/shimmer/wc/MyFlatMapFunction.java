package com.shimmer.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Created by Hui Li on 2021/3/15 21:09
 */
public class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {

    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] words = s.split(" ");
        for (String word: words){
            collector.collect(new Tuple2<String, Integer>(word, 1));
        }
    }
}
