package com.cetc28.seu.spark.query.model.coprocessor;

import java.util.ArrayList;
import java.util.List;

import com.cetc28.seu.hbase.HbaseTool;
import com.cetc28.seu.spark.query.result.LocalAnswerSet;
import com.google.protobuf.ByteString;
/**
 * 根据主题和关键字来查询
 * @author Think
 *
 */
public class SearchSubjectKeyWords {
	public LocalAnswerSet getSubjectAKeyWords(List<String> nameEntity, List<String> subjects){
		long start = System.currentTimeMillis();
		List<List<ByteString>> index = new ArrayList<List<ByteString>>();
		String family = "SubjectsKeyQuery";
		
		index.add(HbaseTool.getInstance().startSearchIndex(family, subjects, new ArrayList<String>(nameEntity)));
		List<ByteString> list = new ArrayList<ByteString>();
		LocalAnswerSet resultSet = new LocalAnswerSet();
		for(List<ByteString> out : index){
			for(ByteString bs : out){
				list.add(bs);
			}
		}
		resultSet.setResults(list);
		long end = System.currentTimeMillis();
		System.out.println("subjects and keywords pay time: " + (end - start));
		return resultSet;
 		
	}
	
	public LocalAnswerSet getKeyWords(List<String> nameEntity){
		long start = System.currentTimeMillis();
		List<List<ByteString>> index = new ArrayList<List<ByteString>>();
		
		index.add(HbaseTool.getInstance().startSearchIndex("", new ArrayList<>(), new ArrayList<String>(nameEntity)));
		List<ByteString> list = new ArrayList<ByteString>();
		LocalAnswerSet resultSet = new LocalAnswerSet();
		for(List<ByteString> out : index){
			for(ByteString bs : out){
				list.add(bs);
			}
		}
		resultSet.setResults(list);
		long end = System.currentTimeMillis();
		System.out.println("keywords pay time: " + (end - start));
		return resultSet;
 		
	}
}
