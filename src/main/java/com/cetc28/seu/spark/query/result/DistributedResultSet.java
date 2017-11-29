package com.cetc28.seu.spark.query.result;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;



import scala.Tuple2;


public class DistributedResultSet extends ResultSet{

	private static final long serialVersionUID = 6476846857571193551L;
	
	private JavaPairRDD<String, Result> resultRDD;
	
	public JavaPairRDD<String, Result> getResultRDD() {
		return resultRDD;
	}
	public void setResultRDD(JavaPairRDD<String, Result> resultRDD) {
		this.resultRDD = resultRDD;
	}
	
	public void show(int num){
		long start = System.currentTimeMillis();
		JavaRDD<Map<String,String>> results=this.resultRDD.map(new Function<Tuple2<String,Result>,Map<String,String>>() {
			
			private static final long serialVersionUID = -7232265844466542327L;
			
			@Override
			public Map<String,String> call(Tuple2<String, Result> v1) throws Exception {			
				Map<String,String> hashMap = new HashMap<String,String>();
				for(Cell cell : v1._2.rawCells())
				{
					String key = Bytes.toString(v1._2.getRow());
					String value = Bytes.toString(CellUtil.cloneFamily(cell))+ " " +Bytes.toString(CellUtil.cloneQualifier(cell)) + " " + Bytes.toString(CellUtil.cloneValue(cell));
					hashMap.put(value, key);
				}
				return hashMap;
			}
		});

		List<Map<String, String>> resultStr=results.take(num);
		long end = System.currentTimeMillis();
		for(Map<String, String> str : resultStr){

			for( Entry<String, String> ele : str.entrySet()){
				System.out.println(ele.getValue()+" : "+ele.getKey());
			}
			
		}
		//System.out.println("pay time: " + (end-start));
		
		//原始方法
//		JavaRDD< Map<String,String>> results=resultRDD.map(new Function<Tuple2<String,Result>, Map<String,String>>() {
//			
//			private static final long serialVersionUID = -7232265844466542327L;
//			
//			@Override
//			public Map<String,String> call(Tuple2<String, Result> v1) throws Exception {
//				Map<String,String> colValue=new HashMap<>();
//				
//				for(String col : askColumns){
//					colValue.put(col, Bytes.toString(v1._2.getValue(Bytes.toBytes("attributes"),Bytes.toBytes(col))));
//				}
//				return colValue;
//			}
//		});
//		for(String col : askColumns){
//			System.out.println("askcol : "+ col);
//		}
//
//		List<Map<String, String>> resultStr=results.take(num);
//		for(Map<String, String> str : resultStr){
//
//			for( Entry<String, String> ele : str.entrySet()){
//				System.out.println(ele.getKey()+" : "+ele.getValue());
//			}
//			
//		}
	}
	
	public void show(){
		long start = System.currentTimeMillis();
		JavaRDD<Map<String,String>> results=this.resultRDD.map(new Function<Tuple2<String,Result>,Map<String,String>>() {
			
			private static final long serialVersionUID = -7232265844466542327L;
			
			@Override
			public Map<String,String> call(Tuple2<String, Result> v1) throws Exception {			
				Map<String,String> hashMap = new HashMap<String,String>();
				for(Cell cell : v1._2.rawCells())
				{
					String key = Bytes.toString(v1._2.getRow());
					String value = Bytes.toString(CellUtil.cloneFamily(cell))+ " " +Bytes.toString(CellUtil.cloneQualifier(cell)) + " " + Bytes.toString(CellUtil.cloneValue(cell));
					hashMap.put(value, key);
				}
				return hashMap;
			}
		});
		//本地一条数据从第二次开始0.6s左右
		Map<String, String> re = results.first();
		long end = System.currentTimeMillis();

		for(Entry<String,String> entry : re.entrySet())
		{
			System.out.println(entry.getValue()+ " " +entry.getKey() );
		}
		System.out.println("pay time: " + (end-start));
	}
	
	
}
