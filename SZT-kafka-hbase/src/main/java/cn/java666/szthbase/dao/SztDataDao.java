package cn.java666.szthbase.dao;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.io.IOException;

import static cn.java666.sztcommon.enums.SztEnum.SZT_NAME_SPACE;
import static cn.java666.sztcommon.enums.SztEnum.SZT_TABLE_CF;
import static cn.java666.sztcommon.enums.SztEnum.SZT_TABLE_NAME;

/**
 * @author Geek
 * @date 2020-04-29 21:36:08
 *
 * 持久层，操作 hbase 数据库，连接复用，降低资源开销
 */

// 赋值给静态成员变量
// @Component
@Slf4j
@Repository
public class SztDataDao {
	
	@Value("${szt.hbase.versions}")
	private Integer version;
	
	@Value("${hbase.zookeeper.quorum}")
	private String hosts;
	
	private Connection connection;
	
	/** 初始化数据库 */
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
		Admin admin = connection.getAdmin();
		try {
			admin.getNamespaceDescriptor(nameSpace);
		} catch (NamespaceNotFoundException e) {
			admin.createNamespace(NamespaceDescriptor.create(nameSpace).build());
		} finally {
			admin.close();
		}
	}
	
	/** 指定列版本数量 */
	public void createTable(String tableName, String... families) throws IOException {
		createTable(tableName, version, families);
	}
	
	public void createTable(String tableName, Integer versions, String... families) throws IOException {
		Admin admin = connection.getAdmin();
		if (admin.tableExists(TableName.valueOf(tableName))) {
			log.warn("table [{}] 已存在", tableName);
			admin.close();
			return;
		}
		HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
		for (String family : families) {
			HColumnDescriptor columnDesc = new HColumnDescriptor(family);
			columnDesc.setMaxVersions(versions);
			tableDesc.addFamily(columnDesc);
		}
		admin.createTable(tableDesc);
		admin.close();
	}
	
	public void putCell(String tableName, String rowKey, String family, String column, String value) throws IOException {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Put put = new Put(rowKey.getBytes());
		put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
		table.put(put);
	}
}
