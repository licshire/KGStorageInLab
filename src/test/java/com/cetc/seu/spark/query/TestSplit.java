package com.cetc.seu.spark.query;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

import com.cetc28.seu.hbase.HbaseConfig;
import com.cetc28.seu.hbase.HbaseTool;

public class TestSplit {

	public static void main(String[] args) {

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
		
//		ExecutorService pool = Executors.newSingleThreadScheduledExecutor();
//		Thread t = new Thread(){
//			@Override
//			public void run() {
//				super.run();
//				HbaseTool.getInstance().rebuildIndexAfterSplit();
//			}
//			
//		};
//		pool.execute(t);
//		pool.shutdown();
		
		// 添加小部分数据进行试验
		try {
			HbaseTool.getInstance().putTableSplit();
//			HbaseTool.getInstance().buildIndexSplit();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
//		while(!pool.isTerminated()){}

//		String regionName = "KG_729,5000,1501292847747.31e327f9865467777627ca443b7a1a91.";
//		
//		//使用多线程执行
//		ExecutorService pool = Executors.newSingleThreadScheduledExecutor();
//		Thread t = new Thread(){
//			@Override
//			public void run() {
//				super.run();
//				HbaseTool.split(regionName);
//			}
//			
//		};
//		pool.execute(t);
//		pool.shutdown();
//		while(!pool.isTerminated()){}
//		HbaseTool.split(regionName);
		
/*		try {
			 List<HRegionInfo> list =
			 HbaseTool.getAdmin().getTableRegions(TableName.valueOf(HbaseConfig.tableName));
//			 for(HRegionInfo h : list){
//				 h.setSplit(true);
//			 //System.out.println(h.getRegionNameAsString()+h.isSplit()+h.isSplitParent(););
//			 }
			 for(HRegionInfo h : list){
				 System.out.println(h.getRegionNameAsString()+h.isSplit()+h.isSplitParent());
			 }
			 System.out.println(list.size());
		 } catch (IOException e) {
		 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }*/


	}

}
