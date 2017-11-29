package com.cetc.seu.spark.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.cetc28.seu.hbase.HbaseConfig;
import com.cetc28.seu.hbase.HbaseTool;

public class TestDelete {

	public static void main(String[] args) {
		String tableName = HbaseConfig.tableName;
		try {
			if (!HbaseTool.getAdmin().tableExists(tableName)) {
				String[] families = { "objects", "attributes", "array_objects", "index" };
				HbaseTool.createTable(tableName, families, 10);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		HTableInterface hTable = null;
		try {
			hTable = HbaseTool.getInstance().getTable(tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		 
		// 添加小部分数据进行试验
		try {
//			HbaseTool.getInstance().putDelete();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		Scan scan = new Scan();
		Filter filter = new FirstKeyOnlyFilter();
		scan.setStartRow(Bytes.toBytes("0001:"));
		scan.setStopRow(Bytes.toBytes("0001;"));
		scan.addFamily(Bytes.toBytes("index"));
		scan.setFilter(filter);
		try {
			ResultScanner rs = HbaseTool.getInstance().getTable(tableName).getScanner(scan);
			List<Delete> deletes = new ArrayList<Delete>();
			for(Result r : rs){
				Delete delete = new Delete(r.getRow());
				deletes.add(delete);
			}
			rs.close();
			HbaseTool.getInstance().getTable(tableName).delete(deletes);
			HbaseTool.getInstance().getTable(tableName).flushCommits();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
