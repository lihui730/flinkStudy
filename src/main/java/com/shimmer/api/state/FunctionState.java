package com.shimmer.api.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Int;

import java.util.Collections;
import java.util.List;

/**
 * Created by Hui Li on 2021/4/11 12:09
 */
public class FunctionState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        DataStreamSource<String> input = env.socketTextStream("localhost", 7777);

        DataStream<Sensor> sensorMap = input.map(line -> {
            String[] split = line.split(",");
            return new Sensor(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        DataStream<Integer> count = sensorMap.map(new MyMapper());


        count.print();

        env.execute();

    }

    public static class MyMapper implements MapFunction<Sensor, Integer>, ListCheckpointed<Integer> {

        private Integer count = 0;
        @Override
        public Integer map(Sensor sensor) throws Exception {
            return count + 1;
        }

        @Override
        public List<Integer> snapshotState(long l, long l1) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> list) throws Exception {
            for (int i: list){
                count += i;
            }
        }
    }
}
