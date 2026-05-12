package cn.java666.etlflink.app;

import cn.java666.etlflink.sink.MyCsvSinkFun;
import cn.java666.etlflink.source.MyRedisSourceFun;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Redis2Csv {
    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String ymd = "2018-09-01";

        env.addSource(new MyRedisSourceFun())
            .filter(x -> {
                JSONObject json = JSON.parseObject(x);
                String dealDate = json.getString("deal_date");
                return dealDate.startsWith(ymd);
            })
            .addSink(new MyCsvSinkFun(ymd));

        env.execute("Redis2Csv");
    }
}
