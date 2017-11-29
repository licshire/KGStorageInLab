package com.cetc28.seu.spark.query.model.coprocessor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;

import com.cetc28.seu.hbase.HbaseConfig;
import com.cetc28.seu.hbase.HbaseTool;
import com.cetc28.seu.query.struct.QueryCondition;
import com.cetc28.seu.rdf.Term;
import com.cetc28.seu.spark.query.model.LeafNode;
import com.cetc28.seu.spark.query.model.QueryNode;
import com.cetc28.seu.spark.query.result.LocalAnswerSet;
import com.cetc28.seu.sparql.FilterClause;
import com.google.protobuf.ByteString;

public class SimpleFilterCoprocessorNode extends LeafNode {

	private static final long serialVersionUID = 5600695106350071907L;
	public static List<FilterClause<?>> filterClause;
	public static HashSet<String> hashSet = new HashSet<String>();
	public String name = HbaseConfig.tableName;
	public SimpleFilterCoprocessorNode(QueryCondition attributes) {
		this.setAttributes(attributes);
	}

	public SimpleFilterCoprocessorNode(QueryCondition attributes, Configuration conf, JavaSparkContext sc) {
		this.setAttributes(attributes);
		QueryNode.hbaseConf = conf;
		QueryNode.sc = sc;
	}

	public SimpleFilterCoprocessorNode(int id, int lchild, int rchild, int parent, Term term, QueryCondition attributes) {
		super(id, lchild, rchild, parent, term);
		this.setAttributes(attributes);
	}

	// 加入filter函数
	public SimpleFilterCoprocessorNode(int id, int lchild, int rchild, int parent, Term term, QueryCondition attributes,
			List<FilterClause<?>> filterClause) {
		super(id, lchild, rchild, parent, term);
		this.setAttributes(attributes);
		SimpleFilterCoprocessorNode.filterClause = filterClause;
	}
	
//	public LocalResultSet query() {
//		// TODO set return columns
//		long start = System.currentTimeMillis();
//		final String family = this.getAttributes().getFamily();
//		//List<String> answers = this.getAttributes().getAnswer();
//		Map<String, String> attributeMap = this.getAttributes().getConditions();
//		List<List<String>> index = new ArrayList<List<String>>();
//		//从文件中读取column名字，用hashSet进行存储
//		if(hashSet == null || hashSet.size() == 0){
//			hashSet = HbaseTool.getInstance().inputFile();
//		}
//		long innerStart = System.currentTimeMillis();
//		System.out.println("");
//		for(Entry<String, String> entry : attributeMap.entrySet())
//		{
//			if(hashSet.contains(entry.getKey())){
//				index.add(HbaseTool.getInstance().startSearchIndex(family,entry.getKey(),entry.getValue()));			
//			}else{
//				System.out.println(family+" column:"+entry.getKey()+" value: "+entry.getValue());
//				index.add(HbaseTool.getInstance().startSearch(family,entry.getKey(),entry.getValue()));
//			}
//		}
//		long innerEnd = System.currentTimeMillis();
//		System.out.println("corprocess time: " + (innerEnd-innerStart));
//		//与条件或者只有一个条件
//		List<String> list = new ArrayList<String>();
//		List<String> temp = new ArrayList<String>();
//
//		int i = 0;
//		for(List<String> out : index){
//			if(i == 0){
//				list = out;
//			}else{
//				//只包含list和out共有的数
//				list.retainAll(out);			
//			}
//			i++;
//		}
//		//批量处理
//		LocalResultSet resultSet = new LocalResultSet();
//		List<Result> listResult = new ArrayList<Result>();
//		List<Row> batch = new ArrayList<Row>();
//		HTableInterface hTable = null;
//		
//		try {
//			hTable = HbaseTool.getInstance().getTable(name);
//		} catch (IOException e1) {
//			e1.printStackTrace();
//		}
//		for (String rowKey : list) {
//			Get get = new Get(Bytes.toBytes(rowKey));
//			batch.add(get);
//		}
//		Object[] results = new Object[batch.size()];
//		try {
//			hTable.batch(batch, results);
//		} catch (IOException e) {
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		for(Object object: results)
//		{
//			Result result = (Result)object;
//			listResult.add(result);
//		}
//		resultSet.setResults(listResult);
//		long end = System.currentTimeMillis();
//		System.out.println("inner pay time: " + (end - start));
//		return resultSet;
//		
//	}
	
	public LocalAnswerSet query() {
		long start = System.currentTimeMillis();
		final String family = this.getAttributes().getFamily();
		//List<String> answers = this.getAttributes().getAnswer();
		Map<String, String> attributeMap = this.getAttributes().getConditions();
		List<List<ByteString>> index = new ArrayList<List<ByteString>>();
//		//从文件中读取column名字，用hashSet进行存储
//		if(hashSet == null || hashSet.size() == 0){
//			hashSet = HbaseTool.getInstance().inputFile();
//		}
		long innerStart = System.currentTimeMillis();
		System.out.println("");
		//原先
//		for(Entry<String, String> entry : attributeMap.entrySet())
//		{
//			if(hashSet.contains(entry.getKey())){
//				index.add(HbaseTool.getInstance().startSearchIndex(family,entry.getKey(),entry.getValue()));			
//			}else{
//				System.out.println(family+" column:"+entry.getKey()+" value: "+entry.getValue());
////				index.add(HbaseTool.getInstance().startSearch(family,entry.getKey(),entry.getValue()));
//			}
//		}
		for(String str : attributeMap.keySet()){
			System.out.println("attributes key:=========" +str);
		}
		
		//在协处理器直接过滤
		index.add(HbaseTool.getInstance().startSearchIndex(family, new ArrayList<String>(attributeMap.keySet()), new ArrayList<String>(attributeMap.values())));
		long innerEnd = System.currentTimeMillis();
		System.out.println("corprocess time: " + (innerEnd-innerStart));
		//与条件或者只有一个条件
		LocalAnswerSet resultSet = new LocalAnswerSet();
		
		List<ByteString> list = new ArrayList<ByteString>();
		for(List<ByteString> out : index){
			for(ByteString bs : out){
				list.add(bs);
			}
		}

		long end = System.currentTimeMillis();
		System.out.println("inner pay time: " + (end - start));
		resultSet.setResults(list);
		return resultSet;
		
	}
	
}
