package com.cetc.seu.spark.query;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.TableName;

import com.cetc28.seu.hbase.HbaseConfig;
import com.cetc28.seu.hbase.HbaseTool;
import com.cetc28.seu.spark.query.model.coprocessor.MultiThread;
import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyFinal.Answer;

public class TestmultiThread {

	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ExecutorService pool = Executors.newCachedThreadPool();
		List<Future<Answer>> tempList = new ArrayList<Future<Answer>>();
//		MultiThread tm = new MultiThread("0111:test:-123:1");
//		Future<Answer> fRes = pool.submit(tm);
//		tempList.add(fRes);
//		 pool.shutdown();
//		 for(Future<Answer> f : tempList){
//			 try {
//				System.out.println(f.get());
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			} catch (ExecutionException e) {
//				e.printStackTrace();
//			}
//		 }
	}

}
