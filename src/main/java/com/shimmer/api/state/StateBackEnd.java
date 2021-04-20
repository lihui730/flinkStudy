package com.shimmer.api.state;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by Hui Li on 2021/4/11 19:51
 */
public class StateBackEnd {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setStateBackend(new FsStateBackend("hdfs://lh-node-1:9000/"));


    }
}
