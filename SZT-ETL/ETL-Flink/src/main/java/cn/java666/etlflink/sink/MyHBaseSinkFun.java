package cn.java666.etlflink.sink;

import cn.hutool.json.JSONUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

public class MyHBaseSinkFun extends RichSinkFunction<String> {
    private static final Logger log = LoggerFactory.getLogger(MyHBaseSinkFun.class);

    private final String tbNameStr;
    private final String ns;
    private final int versions;
    private final String zk;
    private final String cf = "info";

    private Connection conn;
    private Table table;

    public MyHBaseSinkFun(String tbNameStr, String ns, int versions) {
        this.tbNameStr = tbNameStr;
        this.ns = ns;
        this.versions = versions;
        this.zk = "cdh231,cdh232,cdh233";
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        var config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zk);
        conn = ConnectionFactory.createConnection(config);
        var admin = conn.getAdmin();

        try {
            admin.getNamespaceDescriptor(ns);
        } catch (NamespaceNotFoundException e) {
            log.warn("hbase [{}] 命名空间不存在，将会新建", ns);
            admin.createNamespace(NamespaceDescriptor.create(ns).build());
        }

        var nsTbName = TableName.valueOf(ns + ":" + tbNameStr);
        if (!admin.tableExists(nsTbName)) {
            var columnDesc = new ColumnFamilyDescriptorBuilder.ModifiableColumnFamilyDescriptor(Bytes.toBytes(cf));
            columnDesc.setMaxVersions(versions);
            var tableDesc = TableDescriptorBuilder.newBuilder(nsTbName)
                .setColumnFamily(columnDesc)
                .build();
            admin.createTable(tableDesc);
        }
        table = conn.getTable(nsTbName);
    }

    @Override
    public void invoke(String value, SinkFunction.Context context) {
        insert(value);
        log.warn("写入 hbase 成功 [{}]", value);
    }

    private void insert(String jsonStr) {
        try {
            var jsonObj = JSONUtil.parseObj(jsonStr);
            String cardNoRe = StringUtils.reverse(jsonObj.getStr("card_no"));

            Set<String> keys = jsonObj.keySet();
            for (String key : keys) {
                String val = jsonObj.getStr(key);
                putCell(cardNoRe, cf, key, val);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void putCell(String rowKey, String family, String column, String value) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (conn != null) {
            conn.close();
        }
    }
}
