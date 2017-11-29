package com.cetc28.seu.spark.query.model.coprocessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.cetc28.seu.hbase.HbaseConfig;
import com.cetc28.seu.hbase.HbaseTool;
import com.cetc28.seu.query.struct.QueryCondition;
import com.cetc28.seu.rdf.RDF;
import com.cetc28.seu.rdf.Term;
import com.cetc28.seu.spark.query.model.LeafNode;
import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyFinal.Answer;
import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyFinal.AnswerInfo;
import com.cetc28.seu.spark.query.result.LocalAnswerSet;
import com.cetc28.seu.sparql.FilterClause;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class SparqlFilterCoprocessorNode extends LeafNode {
	private static final long serialVersionUID = 582891289243864013L;
	public static List<FilterClause<?>> filterClause;
	public String name = HbaseConfig.tableName;

	public SparqlFilterCoprocessorNode(int id, int lchild, int rchild, int parent, Term term, QueryCondition queryCondition,List<FilterClause<?>> filterClause) 
	{
		super(id, lchild, rchild, parent, term);
		SparqlFilterCoprocessorNode.filterClause = filterClause;
		this.setAttributes(queryCondition);
	}
	
	@Override
	public LocalAnswerSet query() {
		long start = System.currentTimeMillis();
		final String family = this.getAttributes().getFamily();
		List<String> answers = this.getAttributes().getAnswer();
		RDF attributeRDF = this.getAttributes().getRdf();
		List<List<ByteString>> index = new ArrayList<List<ByteString>>();
		List<String> columnList = new ArrayList<>();
		String column = attributeRDF.getPredict().getValue().split(":")[1];
		columnList.add(column);
		index.add(HbaseTool.getInstance().startSearchIndex(family,columnList,new ArrayList<>()));
		
		//与条件或者只有一个条件
		LocalAnswerSet resultSet = new LocalAnswerSet();
		List<ByteString> list = new ArrayList<ByteString>();
		int count = 0;
		for(List<ByteString> out : index){
			for(ByteString bs : out){
				//filter条件过滤
				count++;
				if(SparqlFilterCoprocessorNode.filterClause.size() == 0){
					list.add(bs);
				}else{
					ArrayList<ByteString> l = new ArrayList<>();
					l.add(bs);
					try {
						if(queryFilter(SparqlFilterCoprocessorNode.filterClause,this.getAttributes(),l)){
							list.add(bs);
						}
					} catch (NumberFormatException e) {
						e.printStackTrace();
					} catch (InvalidProtocolBufferException e) {
						e.printStackTrace();
					}
				}
			}
		}		
		resultSet.setResults(list);
		long end = System.currentTimeMillis();
		System.out.println("inner pay time: " + (end - start));
		return resultSet;
		
	}
	
	public Boolean queryFilter(List<FilterClause<?>> filterClause, QueryCondition queryCondition, ArrayList<ByteString> result) throws NumberFormatException, InvalidProtocolBufferException
	{
		//scan的过滤条件，判断是主语过滤还是宾语过滤
		String columnFamily = queryCondition.getFamily();
		String[] column = queryCondition.getRdf().getPredict().getValue().split(":");
		Map<FilterClause<?>,String> map = new HashMap<FilterClause<?>,String>();
		Boolean judgeCondition = false;
		for(FilterClause<?> localFilterClause : filterClause)
		{
			if(localFilterClause.getTerm().equals(queryCondition.getRdf().getSubject()))
			{
				map.put(localFilterClause, "subject");
			}
			if(localFilterClause.getTerm().equals(queryCondition.getRdf().getObject()))
			{
				map.put(localFilterClause, "object");
			}
		}
		for(Entry<FilterClause<?>, String> entry : map.entrySet())
		{
			String row = Answer.parseFrom(result.get(0)).getRow();
			//对filter条件的主语进行搜索，主语目前只能判断=
			if(entry.getValue().equals("subject"))
			{
				switch(entry.getKey().getSymbol())
				{
					case "=":
						if(entry.getKey().getValue().equals(row))
						{
							judgeCondition = true;
						}
						break;
					
				}
			}
			//对filter条件的谓语进行搜索
			if(entry.getValue().equals("object"))
			{
				switch(entry.getKey().getSymbol())
				{
					case ">":
						if(entry.getKey().getValue() instanceof Integer){
							for(AnswerInfo ans : Answer.parseFrom(result.get(0)).getAiList()){
								if(ans.getColumnFamily().equals(columnFamily) && ans.getColumn().equals(column[1])){
									if(Integer.parseInt(ans.getValue()) > (int)entry.getKey().getValue()){
										judgeCondition = true;
										break;
									}
								}
								
							}
						}
						break;
					case "<":
						if(entry.getKey().getValue() instanceof Integer){
							for(AnswerInfo ans : Answer.parseFrom(result.get(0)).getAiList()){
								if(ans.getColumnFamily().equals(columnFamily) && ans.getColumn().equals(column[1])){
									if(Integer.parseInt(ans.getValue()) < (int)entry.getKey().getValue()){
										judgeCondition = true;
										break;
									}
								}
								
							}
						}
						break;
					case "=":
						if(entry.getKey().getValue() instanceof Integer){
							for(AnswerInfo ans : Answer.parseFrom(result.get(0)).getAiList()){
								if(ans.getColumnFamily().equals(columnFamily) && ans.getColumn().equals(column[1])){
									if(Integer.parseInt(ans.getValue()) == (int)entry.getKey().getValue()){
										judgeCondition = true;
										break;
									}
								}
								
							}
						}else if(entry.getKey().getValue() instanceof String){
							for(AnswerInfo ans : Answer.parseFrom(result.get(0)).getAiList()){
								if(ans.getColumnFamily().equals(columnFamily) && ans.getColumn().equals(column[1])){
									if(ans.getValue().equals(entry.getKey().getValue())){
										judgeCondition = true;
										break;
									}
								}
								
							}
						}
						break;
					case ">=":
						if(entry.getKey().getValue() instanceof Integer){
							for(AnswerInfo ans : Answer.parseFrom(result.get(0)).getAiList()){
								if(ans.getColumnFamily().equals(columnFamily) && ans.getColumn().equals(column[1])){
									if(Integer.parseInt(ans.getValue()) >= (int)entry.getKey().getValue()){
										judgeCondition = true;
										break;
									}
								}
								
							}
						}
						break;
					case "<=":
						if(entry.getKey().getValue() instanceof Integer){
							for(AnswerInfo ans : Answer.parseFrom(result.get(0)).getAiList()){
								if(ans.getColumnFamily().equals(columnFamily) && ans.getColumn().equals(column[1])){
									if(Integer.parseInt(ans.getValue()) <= (int)entry.getKey().getValue()){
										judgeCondition = true;
										break;
									}
								}
								
							}
						}
						break;
					case "!=":
						if(entry.getKey().getValue() instanceof Integer){
							for(AnswerInfo ans : Answer.parseFrom(result.get(0)).getAiList()){
								if(ans.getColumnFamily().equals(columnFamily) && ans.getColumn().equals(column[1])){
									if(Integer.parseInt(ans.getValue()) != (int)entry.getKey().getValue()){
										judgeCondition = true;
										break;
									}
								}
								
							}
						}
						break;
				}
			}
		}
		return judgeCondition;
	}
	
}
