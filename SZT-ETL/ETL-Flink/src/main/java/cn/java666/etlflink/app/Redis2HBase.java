package cn.java666.etlflink.app;

import cn.java666.etlflink.sink.MyHBaseSinkFun;
import cn.java666.etlflink.source.MyRedisSourceFun;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Redis2HBase {
    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new MyRedisSourceFun())
            .addSink(new MyHBaseSinkFun("flink2hbase", "flink", 10));

        env.execute("Redis2HBase");
    }
}
