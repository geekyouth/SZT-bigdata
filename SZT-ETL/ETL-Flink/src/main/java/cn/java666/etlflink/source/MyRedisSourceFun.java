package cn.java666.etlflink.source;

import cn.java666.etlflink.util.RedisUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import redis.clients.jedis.Jedis;

public class MyRedisSourceFun extends RichSourceFunction<String> {
    private Jedis client;
    private volatile boolean isRunning = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        client = RedisUtil.getJedisClient();
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        for (int i = 1; i <= 1337 && isRunning; i++) {
            String v = client.hget("szt:pageJson", String.valueOf(i));
            JSONObject json = JSON.parseObject(v);
            var array = json.getJSONArray("data");
            if (array.size() != 1000) {
                System.err.println(" ----- array size error ---- i=" + i);
            }
            for (int j = 0; j < array.size(); j++) {
                String xStr = array.getString(j);
                JSONObject data = JSON.parseObject(xStr);
                if (data.size() == 11) {
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(xStr);
                    }
                }
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        close();
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
