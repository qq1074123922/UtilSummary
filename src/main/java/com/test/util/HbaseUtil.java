package com.test.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseUtil {

	HBaseAdmin admin = null;
	Configuration conf = null;

	/**
	 * 构造函数加载配置
	 */
	public HbaseUtil() {
		conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "192.168.1.176:2181");
		conf.set("hbase.rootdir", "hdfs://192.168.1.176:9000/hbase");
		try {
			admin = new HBaseAdmin(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		HbaseUtil hbase = new HbaseUtil();
		// 创建一张表
		// hbase.createTable("stu","cf");
		// //查询所有表名
		hbase.getALLTable();
		// //往表中添加一条记录
		// hbase.addOneRecord("stu","key1","cf","name","zhangsan");
		// hbase.addOneRecord("stu","key1","cf","age","24");
		// //查询一条记录
		// hbase.getKey("stu","key1");
		// //获取表的所有数据
		// hbase.getALLData("stu");
		// //删除一条记录
		// hbase.deleteOneRecord("stu","key1");
		// //删除表
		// hbase.deleteTable("stu");
		// scan过滤器的使用
		// hbase.getScanData("stu","cf","age");
		// rowFilter的使用
		// 84138413_20130313145955
	}

	/**
	 * rowFilter的使用
	 * 
	 * @param tableName
	 * @param reg
	 * @throws Exception
	 */
	public void getRowFilter(String tableName, String reg) throws Exception {
		HTable hTable = new HTable(conf, tableName);
		Scan scan = new Scan();
		// Filter
		RowFilter rowFilter = new RowFilter(CompareOp.NOT_EQUAL,
				new RegexStringComparator(reg));
		scan.setFilter(rowFilter);
		ResultScanner scanner = hTable.getScanner(scan);
		for (Result result : scanner) {
			System.out.println(new String(result.getRow()));
		}
	}

	public void getScanData(String tableName, String family, String qualifier)
			throws Exception {
		HTable hTable = new HTable(conf, tableName);
		Scan scan = new Scan();
		scan.addColumn(family.getBytes(), qualifier.getBytes());
		ResultScanner scanner = hTable.getScanner(scan);
		for (Result result : scanner) {
			if (result.raw().length == 0) {
				System.out.println(tableName + " 表数据为空！");
			} else {
				for (KeyValue kv : result.raw()) {
					System.out.println(new String(kv.getKey()) + "\t"
							+ new String(kv.getValue()));
				}
			}
		}
	}

	private void deleteTable(String tableName) {
		try {
			if (admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				System.out.println(tableName + "表删除成功！");
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println(tableName + "表删除失败！");
		}

	}

	/**
	 * 删除一条记录
	 * 
	 * @param tableName
	 * @param rowKey
	 */
	public void deleteOneRecord(String tableName, String rowKey) {
		HTablePool hTablePool = new HTablePool(conf, 1000);
		HTableInterface table = hTablePool.getTable(tableName);
		Delete delete = new Delete(rowKey.getBytes());
		try {
			table.delete(delete);
			System.out.println(rowKey + "记录删除成功！");
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println(rowKey + "记录删除失败！");
		}
	}

	/**
	 * 获取表的所有数据
	 * 
	 * @param tableName
	 */
	public void getALLData(String tableName) {
		try {
			HTable hTable = new HTable(conf, tableName);
			Scan scan = new Scan();
			ResultScanner scanner = hTable.getScanner(scan);
			for (Result result : scanner) {
				if (result.raw().length == 0) {
					System.out.println(tableName + " 表数据为空！");
				} else {
					for (KeyValue kv : result.raw()) {
						System.out.println(new String(kv.getKey()) + "\t"
								+ new String(kv.getValue()));
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * 查询一条记录
	 * 
	 * @param tableName
	 * @param rowKey
	 */
	@SuppressWarnings("deprecation")
	public void getKey(String tableName, String rowKey) {
		HTablePool hTable = new HTablePool(conf, 1000);
		HTableInterface table = hTable.getTable(tableName);
		Get get = new Get(rowKey.getBytes());
		try {
			Result rs = table.get(get);
			if (rs.raw().length == 0) {
				System.out.println("不存在关键字为 " + rowKey + " 的行！");
			} else {
				for (KeyValue kv : rs.raw()) {
					System.out.println(new String(kv.getKey()) + "\t"
							+ new String(kv.getValue()));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 添加一条记录
	public void put(String tableName, String row, String columnFamily,
			String column, String data) throws IOException {
		HTablePool hTablePool = new HTablePool(conf, 1000);
		HTableInterface table = hTablePool.getTable(tableName);
		Put p1 = new Put(Bytes.toBytes(row));
		p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
				Bytes.toBytes(data));
		table.put(p1);
		System.out.println("put'" + row + "'," + columnFamily + ":" + column
				+ "','" + data + "'");
	}

	/**
	 * 往表中添加一条记录
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param column
	 * @param value
	 */
	public boolean addOneRecord(String tableName, String rowKey, String family,
			String column, byte[] value) {
		HTablePool hTable = new HTablePool(conf, 1000);
		HTableInterface table = hTable.getTable(tableName);
		Put put = new Put(rowKey.getBytes());
		put.add(family.getBytes(), column.getBytes(), value);
		try {
			table.put(put);
			System.out.println("添加记录 " + rowKey + " 成功！");
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("添加记录 " + rowKey + " 成功！");
			return false;
		}
	}

	/**
	 * 查询所有表名
	 * 
	 * @return
	 * @throws Exception
	 */
	public List<String> getALLTable() throws Exception {
		ArrayList<String> tables = new ArrayList<String>();
		if (admin != null) {
			HTableDescriptor[] listTables = admin.listTables();
			if (listTables.length > 0) {
				for (HTableDescriptor tableDesc : listTables) {
					tables.add(tableDesc.getNameAsString());
					System.out.println(tableDesc.getNameAsString());
				}
			}
		}
		return tables;
	}

	/**
	 * 创建一张表
	 * 
	 * @param tableName
	 * @param column
	 * @throws Exception
	 */
	public void createTable(String tableName, String column) throws Exception {
		if (admin.tableExists(tableName)) {
			System.out.println(tableName + "表已经存在！");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			tableDesc.addFamily(new HColumnDescriptor(column));
			admin.createTable(tableDesc);
			System.out.println(tableName + "表创建成功！");
		}
	}
}
