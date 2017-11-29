package com.cetc.seu.spark.query;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.cetc28.seu.hbase.HbaseConfig;
import com.cetc28.seu.hbase.HbaseTool;

public class TestRowDistribute {

	public static void main(String[] args) {
		String tableName = HbaseConfig.tableName;
		try {
			if (!HbaseTool.getAdmin().tableExists(tableName)) {
				String[] families = { "objects", "attributes", "array_objects", "index" };
//				HbaseTool.createTableTest(tableName, families);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		HTableInterface hTable = null;
		try {
			hTable = HbaseTool.getInstance().getTable(tableName);
//			Put put = new Put(Bytes.toBytes("0111:tast:-123:1"));
//			put.add(Bytes.toBytes("attributes"), Bytes.toBytes("data"),Bytes.toBytes("zhu123"));
//			put.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu0"));
//			hTable.put(put);
//			hTable.flushCommits();
//			Put put1 = new Put(Bytes.toBytes("0111:abst:-123:1"));
//			put1.add(Bytes.toBytes("attributes"), Bytes.toBytes("data"),Bytes.toBytes("zhu123"));
//			put1.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu0"));
//			hTable.put(put1);
//			hTable.flushCommits();
//			Put put2 = new Put(Bytes.toBytes("0111:11st:-123:1"));
//			put2.add(Bytes.toBytes("attributes"), Bytes.toBytes("data"),Bytes.toBytes("zhu123"));
//			put2.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu0"));
//			hTable.put(put2);
//			hTable.flushCommits();
			Put put = new Put(Bytes.toBytes("0111:ttst:-123:1"));
			put.add(Bytes.toBytes("attributes"), Bytes.toBytes("data"),Bytes.toBytes("zhu123"));
			put.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu0"));
			hTable.put(put);
			hTable.flushCommits();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
