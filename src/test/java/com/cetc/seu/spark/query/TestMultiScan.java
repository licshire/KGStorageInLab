package com.cetc.seu.spark.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import com.cetc28.seu.hbase.HbaseConfig;
import com.cetc28.seu.hbase.HbaseTool;
import com.cetc28.seu.spark.query.model.coprocessor.MultiThread;
import com.google.protobuf.ByteString;
import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyFinal.Answer;
import com.cetc28.seu.spark.query.result.LocalAnswerSet;

public class TestMultiScan {

	public static void main(String[] args) {
		String tableName = "TestMultiScan";
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
		Put put = new Put(Bytes.toBytes("0222:test:-123:1"));
		put.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("wayne"));
    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu0"));
//		put.add(Bytes.toBytes("attributes"), Bytes.toBytes("age"),Bytes.toBytes(i+""));
//		put.add(Bytes.toBytes("objects"), Bytes.toBytes("parent"),Bytes.toBytes((prefix+":"+1000)+""));
		put.add(Bytes.toBytes("attributes"), Bytes.toBytes("car"),Bytes.toBytes("Ferrari"));
		try {
			hTable.put(put);
			hTable.flushCommits();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		Put put = new Put(Bytes.toBytes("0111:test:-123:1"));
//		put.add(Bytes.toBytes("attributes"), Bytes.toBytes("data"),Bytes.toBytes("zhu123"));
//		put.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu0"));
//		try {
//			hTable.put(put);
//			hTable.flushCommits();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		Get get = new Get(Bytes.toBytes("0111:test:-123:1"));
		List<Filter> l = new ArrayList<Filter>();
		List<String> columnList = new ArrayList<String>();
		columnList.add("className");
		columnList.add("bdnm");
		
		List<String> valueList = new ArrayList<String>();
		valueList.add("zhu0");
		valueList.add("wayne");
		
		for(int i = 0; i < columnList.size(); i++){
			SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("attributes"),Bytes.toBytes(columnList.get(i)),CompareOp.EQUAL,Bytes.toBytes(valueList.get(i)));
			//get.addColumn(Bytes.toBytes(columnList.get(i)), Bytes.toBytes(valueList.get(i)));
			filter.setFilterIfMissing(true);
			l.add(filter);
		}
//		SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("attributes"),Bytes.toBytes("className"),CompareOp.EQUAL,Bytes.toBytes("zhu0"));
//		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("attributes"),Bytes.toBytes("bdnm"),CompareOp.EQUAL,Bytes.toBytes("wayne"));
//		l.add(filter);
//		l.add(filter1);
//		filter.setFilterIfMissing(true);
//		filter1.setFilterIfMissing(true);

		FilterList f = new FilterList(l);
		get.setFilter(f);
		try {
			Result result = hTable.get(get);
			String ss = Bytes.toString(result.getRow());
			System.out.println(ss);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		ExecutorService pool = Executors.newCachedThreadPool();
//		List<Future<ByteString>> tempList = new ArrayList<Future<ByteString>>();		
//		TestMultiThread tm = new TestMultiThread("0111:test:-123:1");
//		
//		Future<ByteString> fRes = pool.submit(tm);
//		tempList.add(fRes);
//		pool.shutdown();
//		List<ByteString> list = new ArrayList<ByteString>();
//		LocalAnswerSet resultSet = new LocalAnswerSet();
//
//		 for(Future<ByteString> f : tempList){
//				try {
//					list.add(f.get());
//				} catch (InterruptedException | ExecutionException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//		 }
//		 resultSet.setResults(list);
//		 resultSet.show(2);
	}

}
