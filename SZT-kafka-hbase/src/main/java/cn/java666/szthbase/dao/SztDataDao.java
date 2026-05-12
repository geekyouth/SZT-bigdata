package cn.java666.szthbase.dao;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.io.IOException;

import static cn.java666.sztcommon.enums.SztEnum.*;

@Slf4j
@Repository
public class SztDataDao {

    @Value("${szt.hbase.versions}")
    private Integer version;

    @Value("${hbase.zookeeper.quorum}")
    private String hosts;

    private Connection connection;

    @PostConstruct
    public void init() {
        try {
            initConnection();
            initNamespace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void initNamespace() throws IOException {
        createNameSpace(SZT_NAME_SPACE.value());
        createTable(SZT_TABLE_NAME.value(), SZT_TABLE_CF.value());
    }

    public void initConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", hosts);
        connection = ConnectionFactory.createConnection(conf);
    }

    public void createNameSpace(String nameSpace) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            try {
                admin.getNamespaceDescriptor(nameSpace);
            } catch (NamespaceNotFoundException e) {
                admin.createNamespace(NamespaceDescriptor.create(nameSpace).build());
            }
        }
    }

    public void createTable(String tableName, String... families) throws IOException {
        createTable(tableName, version, families);
    }

    public void createTable(String tableName, Integer versions, String... families) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                log.warn("table [{}] 已存在", tableName);
                return;
            }
            var tableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
            for (String family : families) {
                var columnDesc = new ColumnFamilyDescriptorBuilder.ModifiableColumnFamilyDescriptor(Bytes.toBytes(family));
                columnDesc.setMaxVersions(versions);
                tableDesc.setColumnFamily(columnDesc);
            }
            admin.createTable(tableDesc.build());
        }
    }

    public void putCell(String tableName, String rowKey, String family, String column, String value) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(rowKey.getBytes());
            put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
            table.put(put);
        }
    }
}
