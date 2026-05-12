package cn.java666.etlflink.app;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Jsons2Redis {
    private static final String SAVE_PATH = "/tmp/szt-data/szt-data-page.jsons";

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var stream = env.readTextFile(SAVE_PATH)
            .filter(line -> !line.isEmpty())
            .map(JSON::parseObject);

        stream.print();

        env.execute("Jsons2Redis");
    }
}
