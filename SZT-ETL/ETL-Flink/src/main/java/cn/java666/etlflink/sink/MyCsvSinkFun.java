package cn.java666.etlflink.sink;

import cn.hutool.core.io.FileUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.StringJoiner;

public class MyCsvSinkFun implements SinkFunction<String> {
    private final String day;
    private final String savePath;

    public MyCsvSinkFun(String day) {
        this.day = day;
        this.savePath = "/tmp/szt-data/szt-data_" + day + ".csv";
    }

    @Override
    public void invoke(String value, Context context) {
        JSONObject json = JSON.parseObject(value);
        String csv = new StringJoiner(",")
            .add(json.getString("deal_date"))
            .add(json.getString("close_date"))
            .add(json.getString("card_no"))
            .add(json.getString("deal_value"))
            .add(json.getString("deal_type"))
            .add(json.getString("company_name"))
            .add(json.getString("car_no"))
            .add(json.getString("station"))
            .add(json.getString("conn_mark"))
            .add(json.getString("deal_money"))
            .add(json.getString("equ_no"))
            .toString();
        FileUtil.appendUtf8String(csv + "\n", savePath);
    }
}
