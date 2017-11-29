package com.cetc28.seu.spark.query.model.coprocessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.cetc28.seu.rdf.RDF;
import com.cetc28.seu.rdf.Term;
import com.cetc28.seu.spark.query.model.InnerNode;
import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyFinal.Answer;
import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyFinal.AnswerInfo;
import com.cetc28.seu.spark.query.result.LocalAnswerSet;
//import com.cetc28.seu.spark.query.result.LocalResultSet;
import com.cetc28.seu.spark.query.result.ResultSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

enum  returnValueLocal{left,right};// 表示返回值
public class JoinerCoprocessorNode extends InnerNode{

	private static final long serialVersionUID = -5515459951672424727L;
	private final static String family = "objects";
	private String joinColumn;
	private returnValueLocal returnV;
	private static int count = 0;
	
	public  JoinerCoprocessorNode() {
		
	}
	
	public JoinerCoprocessorNode(int id, int parent, Term term, String joinColumn) {
		super(id, -1, -1, parent, term);
		this.joinColumn=joinColumn;
	}

	public JoinerCoprocessorNode(int id, int parent, Term childTerm, RDF childRDF, String joinColumn) {
		super(id, -1, -1, parent, childTerm);
		constructReturn(childRDF);
		this.joinColumn=joinColumn;
	}

	public void constructReturn(RDF rdf){
		System.out.println("subject: " + rdf.getSubject().getValue());
		System.out.println("Term: " + this.getTerm().getValue());
		if(this.getTerm().getValue().equals(rdf.getSubject().getValue())){
			returnV=returnValueLocal.left;
		}else{
			returnV=returnValueLocal.right;
		}
	}
	
	@Override
	public ResultSet query() {
		// TODO local query join using hashMap
		// 1. read from the left (subject) , save to a hashmap
		LocalAnswerSet locRes=new LocalAnswerSet();
		List<ByteString> res=new ArrayList<>();
		LocalAnswerSet subject=(LocalAnswerSet) this.getLelfInputResult();
		List<ByteString> subjectResults=subject.getResults();
		HashMap<String,ByteString> maps=new HashMap<>();
		System.out.println("count============" + count++);
		System.out.println(this.joinColumn);
		System.out.println(returnV.toString());

		
		for(ByteString result : subjectResults){
			Answer ans = null;
			try {
				ans = Answer.parseFrom(result);
			} catch (InvalidProtocolBufferException e) {
				e.printStackTrace();
			}
			for(AnswerInfo ai : ans.getAiList()){
				String cf = ai.getColumnFamily();
				String c = ai.getColumn();
				if(cf.equals(family) && c.equals(joinColumn)){
					String row = ai.getValue();
					maps.put(row,result);
//					System.out.println(row+" "+cf+":"+c+" ");
				}
			}
		}
		List<ByteString> objectResult=((LocalAnswerSet) this.getRightInputResult()).getResults();
		boolean flag=false;
		for(ByteString result : objectResult){
			Answer ans = null;
			try {
				ans = Answer.parseFrom(result);
			} catch (InvalidProtocolBufferException e) {
				e.printStackTrace();
			}
			String key = ans.getRow();
			if(maps.containsKey(key)){
				if(flag||returnV.equals(returnValueLocal.left)){
					flag=true;
					res.add(maps.get(key));
				}else{
					res.add(result);
				}
			}
		}
		locRes.setResults(res);
		return locRes;
	}
	
	
	public String getJoinColumn() {
		return joinColumn;
	}


	public void setJoinColumn(String joinColumn) {
		this.joinColumn = joinColumn;
	}

	public returnValueLocal getReturnV() {
		return returnV;
	}

	public void setReturnV(returnValueLocal returnV) {
		this.returnV = returnV;
	}
	
//	@Override
//	public ResultSet query() {
//		// TODO local query join using hashMap
//		// 1. read from the left (subject) , save to a hashmap
//		LocalResultSet locRes=new LocalResultSet();
//		List<Result> res=new ArrayList<>();
//		LocalResultSet subject=(LocalResultSet) this.getLelfInputResult();
//		List<Result> subjectResults=subject.getResults();
//		HashMap<String,Result> maps=new HashMap<>();
//		System.out.println(count++);
//		System.out.println(this.joinColumn);
//		System.out.println(returnV.toString());
//
//		
//		for(Result result : subjectResults){
//			System.out.println("sub: " + Bytes.toString(result.getValue(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"))));
//			byte[] key=result.getValue(Bytes.toBytes(family), Bytes.toBytes(joinColumn));
//			maps.put(Bytes.toString(key),result);
//		}
//		List<Result> objectResult=((LocalResultSet) this.getRightInputResult()).getResults();
//		boolean flag=false;
//		for(Result result : objectResult){
//			System.out.println("obj: " + Bytes.toString(result.getValue(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"))));
//			String key = Bytes.toString(result.getRow());
//			if(maps.containsKey(key)){
//				if(flag||returnV.equals(returnValueLocal.left)){
//					flag=true;
//					res.add(maps.get(key));
//				}else{
//					res.add(result);
//				}
//			}
//		}
//		locRes.setResults(res);
//		return locRes;
//	}

}
