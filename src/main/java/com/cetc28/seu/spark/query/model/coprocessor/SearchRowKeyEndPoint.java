package com.cetc28.seu.spark.query.model.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ProtoUtil;

import com.cetc28.seu.hbase.HbaseConfig;
import com.cetc28.seu.hbase.HbaseTool;
import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyFinal.Answer;
import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyFinal.AnswerInfo;
import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyFinal.SearchRequest;
import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyFinal.SearchResponse;
import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyFinal.SearchService;
import com.google.protobuf.ByteString;
//import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKey.SearchRequest;
//import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKey.SearchResponse;
//import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKey.SearchService;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;



/**
 * 使用协处理器通过scan索引表的方法找到对应的行键
 * @author Think
 *
 */
public class SearchRowKeyEndPoint extends SearchService implements Coprocessor, CoprocessorService {

	private RegionCoprocessorEnvironment env;
	private static final Log LOG = LogFactory.getLog(SearchRowKeyEndPoint.class);
	
	@Override
	public Service getService() {
		return this;
	}
	

	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		if (env instanceof RegionCoprocessorEnvironment) {
			this.env = (RegionCoprocessorEnvironment) env;
		} else {
			throw new CoprocessorException("Must be loaded on a table region!");
		}
		
	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
	}
	
	//TODO 关键字查询
	public List<ByteString> keyWordQueryWithoutIndex(String startKeyPrefix, List<String> valueList){
		List<ByteString> arrayList = new ArrayList<>();
		Scan scan = new Scan();
		for(String value : valueList){
			String startKey = startKeyPrefix+":";
			scan.setStartRow(Bytes.toBytes(startKey));
			scan.addFamily(Bytes.toBytes("attributes"));
			scan.addFamily(Bytes.toBytes("objects"));
			scan.addFamily(Bytes.toBytes("array_objects"));
			Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(value));
			scan.setFilter(filter);
			arrayList = scanQueryWithOutIndex(scan,new ArrayList<>(),new ArrayList<>());
		}
		return arrayList;
	}
 
	//TODO 查询但是没有建索引
	public List<ByteString> scanQueryWithOutIndex(Scan scan, List<String> columnList, List<String> valueList){
		List<ByteString> arrayList = new ArrayList<ByteString>();
		ExecutorService pool = Executors.newCachedThreadPool();
		List<Future<ByteString>> tempList = new ArrayList<Future<ByteString>>();
		
		InternalScanner scanner = null; 
		boolean hasMore = false;
		int count = 0;
		try {
			scanner = env.getRegion().getScanner(scan);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		List<Cell> result = new ArrayList<Cell>();
		do{
			 //模糊查询最多20个返回
			 if(count > 20){
				 break;
			 }
			 try {
				hasMore = scanner.next(result);
//				 LOG.info("SearchRowKeyWithoutIndex hasMore:==================== " + hasMore + result.size());
				 if(result.size() == 0)
				 {
					 break;
				 }
				 else
				 {
					 String RowKey = Bytes.toString(CellUtil.cloneRow(result.get(0)));
					 //自己写的多线程
					 MultiThread tm = new MultiThread(RowKey,env.getRegion());
					 Future<ByteString> fRes = pool.submit(tm);
					 tempList.add(fRes);
					 result.clear();
					 count++;
				 }
			} catch (IOException e) {
				e.printStackTrace();
			}
		
		 }while(hasMore);	
		 pool.shutdown();
		 for(Future<ByteString> f : tempList){
			 try {
				arrayList.add(f.get());
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		 }
		 try {
			scanner.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		 return arrayList;
	}

	//TODO 关键字查询
	public List<ByteString> keyWordQuery(String startKeyPrefix, List<String> valueList){
		List<ByteString> arrayList = new ArrayList<ByteString>();
		ExecutorService pool = Executors.newCachedThreadPool();
		List<Future<ByteString>> tempList = new ArrayList<Future<ByteString>>();
		Scan scan = new Scan();
		for(String value : valueList){
			String startKey = startKeyPrefix+":"+value+":";
			String stopKey = startKeyPrefix+":"+value+";";
			scan.setStartRow(Bytes.toBytes(startKey));
			scan.setStopRow(Bytes.toBytes(stopKey));
			InternalScanner scanner = null;
			try {
				scanner = env.getRegion().getScanner(scan);
				List<Cell> result = new ArrayList<Cell>();
				 //关键字查询（包括模糊查询）
				 boolean hasMore1 = false;
				 do{
					 String returnRowKey = "";
					 hasMore1 = scanner.next(result);
					 //关键字不匹配，比如原先数据库有zhu123,而你查的是zhu1
					 if(result.size() == 0)
					 {
						 result.clear();
						 Scan dimScan = new Scan();
						 dimScan.addFamily(Bytes.toBytes("attributes"));
						 dimScan.addFamily(Bytes.toBytes("objects"));
						 dimScan.addFamily(Bytes.toBytes("array_objects"));
						 Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(value));
						 dimScan.setFilter(filter);
						 scanner = env.getRegion().getScanner(dimScan);
						 boolean hasMore = false;
						 int count = 0;
						 do{
							 //模糊查询最多20个返回
							 if(count > 20){
								 break;
							 }
							 hasMore = scanner.next(result);
							 if(result.size() == 0)
							 {
								 break;
							 }
							 else
							 {
								 String RowKey = Bytes.toString(CellUtil.cloneRow(result.get(0)));
								 //自己写的多线程
								 MultiThread tm = new MultiThread(RowKey,env.getRegion());
								 Future<ByteString> fRes = pool.submit(tm);
								 tempList.add(fRes);
								 result.clear();
								 count++;
							 }
						 }while(hasMore);						 
					 //关键字完全匹配
					 }else{
						 String temp = Bytes.toString(CellUtil.cloneRow(result.get(0)));
						 String[] partRowKey = temp.split("_");
						 returnRowKey = partRowKey[1];
						//自己写的多线程
						 MultiThread tm = new MultiThread(returnRowKey,env.getRegion());
						 Future<ByteString> fRes = pool.submit(tm);
						 tempList.add(fRes);						 
						 result.clear();
					 }
				 }while(hasMore1);
				 
				 pool.shutdown();
				 for(Future<ByteString> f : tempList){
					 try {
						arrayList.add(f.get());
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}
				 }				 
				 //关闭scanner
				 scanner.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return arrayList;
	}
	
	//TODO 模糊查询，如"attributes:data"
	public List<ByteString> dimQueryWithoutIndex(String startKeyPrefix,String family,List<String> columnList){
		List<ByteString> arrayList = new ArrayList<>();
		Scan scan = new Scan();
		String startKey = startKeyPrefix+":";
		scan.setStartRow(Bytes.toBytes(startKey));
		List<Filter> listFilter = new ArrayList<>();
		Filter filter1 = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(family)));
		listFilter.add(filter1);
		for(String column : columnList){
			Filter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(column)));
			listFilter.add(filter);
		}
		Filter f = new FilterList(listFilter);
		scan.setFilter(f);
		arrayList = scanQuery(scan,family,new ArrayList<>(),new ArrayList<>());
		return arrayList;
	}
	
	//TODO 模糊查询，如"attributes:bdnm"
	public List<ByteString> dimQuery(String startKeyPrefix,String family,List<String> columnList){
		List<ByteString> arrayList = new ArrayList<>();
		Scan scan = new Scan();
		if(columnList.size() == 1){
			scan.addFamily(Bytes.toBytes("index"));
			String temp = family+":"+columnList.get(0);
			Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(temp));
			scan.setFilter(filter);
		}else{
			for(String column : columnList){
				scan.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes(column));
			}
		}
		arrayList = scanQuery(scan,family,new ArrayList<>(),new ArrayList<>());
		return arrayList;
	}
	
	//TODO 获取值
	public List<ByteString> scanQuery(Scan scan, String family, List<String> columnList, List<String> valueList){
		List<ByteString> arrayList = new ArrayList<ByteString>();
		ExecutorService pool = Executors.newCachedThreadPool();
		List<Future<ByteString>> tempList = new ArrayList<Future<ByteString>>();
		InternalScanner scanner = null; 
		try {
			scanner = env.getRegion().getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}
		List<Cell> result = new ArrayList<Cell>();
		 boolean hasMore = false;
		 do{
			 String returnRowKey = "";
			 try {
				hasMore = scanner.next(result);
				if(result.size() == 0)
				{
					break;
				}
				else
				{
					 String temp = Bytes.toString(CellUtil.cloneRow(result.get(0)));
					 if(temp.contains("_")){
						 String[] partRowKey = temp.split("_");
						 returnRowKey = partRowKey[1];
//						 LOG.info("returnRowKey:==================== " + returnRowKey);
					 }else{
						 returnRowKey = temp;
					 }
					 //自己写的多线程
					 MultiThread tm = new MultiThread(returnRowKey, env.getRegion(),family, columnList, valueList);
					 Future<ByteString> fRes = pool.submit(tm);
					 tempList.add(fRes);
					 result.clear();
	
				 }
			} catch (IOException e) {
				e.printStackTrace();
			}
			 
		 }while(hasMore);
		 
		 pool.shutdown();
		 for(Future<ByteString> f : tempList){
			 try {
				arrayList.add(f.get());
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		 }
		 try {
			scanner.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		 return arrayList;

	}
	
	
	/**
	 * TODO 主题关键字查询
	 * @param startKeyPrefix
	 * @param subjects
	 * @param valueList
	 * @return
	 */
	public List<ByteString> subjectKeyQuery(String startKeyPrefix, List<String> subjects, List<String> valueList){
		List<ByteString> arrayList = new ArrayList<ByteString>();
		ExecutorService pool = Executors.newCachedThreadPool();
		List<Future<ByteString>> tempList = new ArrayList<Future<ByteString>>();
		Scan scan = new Scan();
		for(String value : valueList){
			String startKey = startKeyPrefix+":"+value+":";
			String stopKey = startKeyPrefix+":"+value+";";
			scan.setStartRow(Bytes.toBytes(startKey));
			scan.setStopRow(Bytes.toBytes(stopKey));
			InternalScanner scanner = null;
			try {
				scanner = env.getRegion().getScanner(scan);
				List<Cell> result = new ArrayList<Cell>();
				 //关键字查询（包括模糊查询）
				 boolean hasMore1 = false;
				 do{
					 String returnRowKey = "";
					 hasMore1 = scanner.next(result);
					 //关键字不匹配，比如原先数据库有zhu123,而你查的是zhu1
					 if(result.size() == 0)
					 {
						 result.clear();
						 Scan dimScan = new Scan();
						 dimScan.addFamily(Bytes.toBytes("attributes"));
						 dimScan.addFamily(Bytes.toBytes("objects"));
						 dimScan.addFamily(Bytes.toBytes("array_objects"));
						 Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(value));
						 dimScan.setFilter(filter);
						 scanner = env.getRegion().getScanner(dimScan);
						 boolean hasMore = false;
						 int count = 0;
						 do{
							 //模糊查询最多20个返回
							 if(count > 20){
								 break;
							 }
							 hasMore = scanner.next(result);
							 if(result.size() == 0)
							 {
								 break;
							 }
							 else
							 {
								 String RowKey = Bytes.toString(CellUtil.cloneRow(result.get(0)));
								 //自己写的多线程
								 MultiThread tm = new MultiThread(RowKey,env.getRegion(),subjects);
								 Future<ByteString> fRes = pool.submit(tm);
								 tempList.add(fRes);
								 result.clear();
								 count++;
							 }
						 }while(hasMore);						 
					 //关键字完全匹配
					 }else{
						 String temp = Bytes.toString(CellUtil.cloneRow(result.get(0)));
						 String[] partRowKey = temp.split("_");
						 returnRowKey = partRowKey[1];
//						 LOG.info("returnRowKey===================" + returnRowKey);
						//自己写的多线程 
						 MultiThread tm = new MultiThread(returnRowKey,env.getRegion(),subjects);
						 Future<ByteString> fRes = pool.submit(tm);
						 tempList.add(fRes);						 
						 result.clear();
					 }
				 }while(hasMore1);
				 
				 pool.shutdown();
				 for(Future<ByteString> f : tempList){
					 try {
						arrayList.add(f.get());
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}
				 }				 
				 //关闭scanner
				 scanner.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return arrayList;
	}
	
	
	@Override
	public void getRowKey(RpcController controller, SearchRequest request, RpcCallback<SearchResponse> done) {
		//获取该region的起始行键,不知道先确认
		if(!env.getRegion().getRegionInfo().getTable().getNameAsString().equals(HbaseConfig.tableName)){
			return;
		}
		String startKeyPrefix = Bytes.toString(env.getRegion().getStartKey());
		
		if(startKeyPrefix.equals("") || startKeyPrefix.isEmpty() || startKeyPrefix.equals("9999a"))
		{
			return;
		}
		else
		{
			//之后修改，可以将做索引的列放入region的起始行键，减少获得htable的时间
			Get getColumn = new Get(Bytes.toBytes("9999a"));
			List<String> columnList = new ArrayList<String>();
			try {
				Result resultColumn = HbaseTool.getInstance().getTable(HbaseConfig.tableName).get(getColumn);
				for(Cell cell : resultColumn.rawCells()){
					columnList.add(Bytes.toString(CellUtil.cloneValue(cell)));
				}
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			boolean judge = false;
			//若其中有一个有索引，则继续，否则跳入...
			String ct = "";
			for(String columnTemp : request.getColumnList()){
				if(columnList.contains(columnTemp)){
					judge = true;
					ct = columnTemp;
					break;
				}
			}
			List<ByteString> returnList = new ArrayList<ByteString>();

			/**
			 * TODO 关键字搜索(管主题），如纯搜索zhu0,没有
			 */
			String family = request.getFamily();
			if(family.equals("SubjectsKeyQuery")){
				returnList = subjectKeyQuery(startKeyPrefix,request.getColumnList(),request.getValueList());
			}else if(judge == true){
//				String family = request.getFamily();
				//TODO 关键字查询，如zhu0
				if(family.equals("") && request.getColumnList().size() == 0){
					returnList = keyWordQuery(startKeyPrefix,request.getValueList());
				//TODO attributes：bdnm ?o查询
				}else if(!family.isEmpty() && request.getColumnList().size() != 0 && request.getValueList().size() == 0){
					returnList = dimQuery(startKeyPrefix,family,request.getColumnList());
				//TODO 简单查询
				}else{
					int n = request.getColumnList().indexOf(ct);
					Scan scan = new Scan();
					String startKey = startKeyPrefix+":"+request.getValue(n)+":"+family+":"+ct;
					String stopKey = startKeyPrefix+":"+request.getValue(n)+":"+family+":"+ct+"`"; //unicode编码比_大
					scan.setStartRow(Bytes.toBytes(startKey));
					scan.setStopRow(Bytes.toBytes(stopKey));
					returnList = scanQuery(scan,family,request.getColumnList(),request.getValueList());
				}
				//TODO 无索引的检索
			}else{
//				String family = request.getFamily();
				String startKeyPrefixChanged = startKeyPrefix.substring(0, 3)+"1";
				if(family.equals("") && request.getColumnList().size() == 0){
					returnList = keyWordQueryWithoutIndex(startKeyPrefix,request.getValueList());
				//TODO attributes：bdnm ?o查询
				}else if(!family.isEmpty() && request.getColumnList().size() != 0 && request.getValueList().size() == 0){
//					LOG.info("attributes：data ?o=====================");
					returnList = dimQueryWithoutIndex(startKeyPrefix,family,request.getColumnList());
				//TODO 简单查询
				}else{
					String startKey = startKeyPrefixChanged+":";
					Scan scan = new Scan();
					scan.setStartRow(Bytes.toBytes(startKey));
					scan.addFamily(Bytes.toBytes("attributes"));
					scan.addFamily(Bytes.toBytes("objects"));
					scan.addFamily(Bytes.toBytes("array_objects"));
					List<Filter> ffl = new ArrayList<>();
					for(int i = 0; i < request.getColumnList().size(); i++){
						SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(request.getColumnList().get(i)), CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(request.getValue(i))));
						filter.setFilterIfMissing(true);
						ffl.add(filter);
					}
					Filter fl = new FilterList(ffl);
					scan.setFilter(fl);
					returnList = scanQuery(scan,family,new ArrayList<String>(),new ArrayList<String>());
				}
			}
			SearchResponse response = null;
			SearchResponse.Builder builder = SearchResponse.newBuilder();
			builder.addAllAnswer(returnList);
			response = builder.build();
	        done.run(response);
		}
	
	}

}