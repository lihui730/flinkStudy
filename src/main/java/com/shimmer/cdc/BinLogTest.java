package com.shimmer.cdc;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;

/**
 * Created by LH on 2021/4/18 10:18
 */
public class BinLogTest {
    public static void main(String[] args) throws Exception {
        final BinaryLogClient client =
                new BinaryLogClient("127.0.0.1", 3306, "root", "Thinker@123");
        client.setBinlogFilename("LAPTOP-LH-bin.000038"); //指定binlog文件
        client.setBinlogPosition(0); //指定从binlog的哪个位置开始读取
        client.registerEventListener(new BinaryLogClient.EventListener() {
            public void onEvent(Event event) {
                System.out.println("===========" + client.getBinlogPosition() + "===========");
                System.out.println("info=" + event.toString());
            }
        });
        client.connect();
    }
}
