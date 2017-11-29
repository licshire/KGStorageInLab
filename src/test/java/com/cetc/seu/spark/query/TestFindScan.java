package com.cetc.seu.spark.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.cetc28.seu.hbase.HbaseConfig;
import com.cetc28.seu.hbase.HbaseTool;

public class TestFindScan {

	public static void main(String[] args) throws IOException {
		String tableName = HbaseConfig.tableName;
		try {
			if (!HbaseTool.getAdmin().tableExists(tableName)) {
				String[] families = { "objects", "attributes", "array_objects", "index" };
				HbaseTool.createTable(tableName, families, 2);
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
		
//		// scan扫描找到该region下的中间点，由于split时间假设是在晚上，因此无需使用协处理统计行数。
//		byte[] startKey = HbaseTool.getAdmin().getTableRegions(TableName.valueOf(HbaseConfig.tableName)).get(2).getStartKey();
//		byte[] endKey = HbaseTool.getAdmin().getTableRegions(TableName.valueOf(HbaseConfig.tableName)).get(2).getEndKey();
//		Filter filter = new FirstKeyOnlyFilter();
//		Scan scan = new Scan();
//		// 从Region的startKey的下一个行键开始搜索，原因：除去索引
//		String start = Bytes.toString(startKey);
//		if (start == null) {
//			try {
//				throw new Exception("不能split");
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//		//String s = start.substring(0, start.length() - 1) + "1";
//		scan.setStartRow(startKey);
//		scan.setStopRow(endKey);
//		scan.addFamily(Bytes.toBytes("attributes"));
//		scan.addFamily(Bytes.toBytes("objects"));
//		scan.addFamily(Bytes.toBytes("array_objects"));
//
//		scan.setFilter(filter);
//		ResultScanner resultScanner = null;
//		ResultScanner resultScanner2 = null;
//		try {
//			resultScanner =  HbaseTool.getInstance().getTable(HbaseConfig.tableName).getScanner(scan);
//			resultScanner2 =  HbaseTool.getInstance().getTable(HbaseConfig.tableName).getScanner(scan);
//		} catch (IOException e1) {
//			e1.printStackTrace();
//		}
//		int rowCount = 0;
//		int halfCount = 0;
//		List<Cell> results = new ArrayList<>();
//		byte[] splitPoint = null;
//		byte[] temp = null;
//		Iterator<Result> result = resultScanner.iterator();
//		Iterator<Result> res = resultScanner2.iterator();
//		while(result.hasNext()){
//			rowCount++;
//			result.next();
//			if(rowCount % 2 == 0){
//				if(res.hasNext()){
//					splitPoint = res.next().getRow();
//				}
//			}
//			
//		}
//		resultScanner.close();
//		resultScanner2.close();
//		System.out.println("splitPoint: " + Bytes.toString(splitPoint));
		
		
		
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes("7498"));
//		scan.addFamily(Bytes.toBytes("attributes"));
//		scan.addFamily(Bytes.toBytes("objects"));
//		scan.addFamily(Bytes.toBytes("array_objects"));

		try {
			ResultScanner rs = hTable.getScanner(scan);
			for(Result res : rs){
				System.out.println(Bytes.toString(res.getRow()));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
