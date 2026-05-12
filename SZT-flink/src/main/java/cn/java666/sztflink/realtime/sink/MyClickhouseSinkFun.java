package cn.java666.sztflink.realtime.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class MyClickhouseSinkFun extends RichSinkFunction<String> {
    private static final Logger log = LoggerFactory.getLogger(MyClickhouseSinkFun.class);

    private final String host;
    private final int port;
    private final String user;
    private final String passwd;

    private Connection conn;
    private Statement stat;

    public MyClickhouseSinkFun(String host) {
        this(host, 8123, "default", "");
    }

    public MyClickhouseSinkFun(String host, int port, String user, String passwd) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.passwd = passwd;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = DriverManager.getConnection("jdbc:clickhouse://" + host + ":" + port, user, passwd);
        stat = conn.createStatement();
    }

    @Override
    public void invoke(String value, SinkFunction.Context context) throws Exception {
        stat.execute("INSERT INTO szt.szt_data VALUES ('" + value + "')");
        log.info("clickhouse save: {}", value);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (conn != null) {
            conn.close();
        }
    }
}
