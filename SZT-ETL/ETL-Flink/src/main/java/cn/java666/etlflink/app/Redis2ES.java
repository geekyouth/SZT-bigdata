package cn.java666.etlflink.app;

import cn.java666.etlflink.source.MyRedisSourceFun;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Redis2ES {
    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new MyRedisSourceFun())
            .print();

        env.execute("Redis2ES");
    }
}
