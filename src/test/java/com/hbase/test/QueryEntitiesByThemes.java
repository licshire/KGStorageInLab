package com.hbase.test;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;

import com.cetc28.seu.hbase.HbaseTool;
import com.cetc28.seu.hbase.IndexTable.SearchIndexTableByCoprocessor;
import com.cetc28.seu.hbase.IndexTable.SparkSearchIndexTable;
import com.cetc28.seu.loading.theme.model.EntityInfo;

import junit.framework.TestCase;

public class QueryEntitiesByThemes extends TestCase implements Serializable{
	

	/**
	 * 
	 */
	private static final long serialVersionUID = 2388752444648443692L;

	public void testQuery() throws IOException
	{
//		final String indexTableName = "index";
//		final String tableName = "experiment";
//			
//		String namedEntityName = "Ferrari";
//		List<String> subjectNames = new ArrayList<String>();
//		subjectNames.add("Good car");
//		subjectNames.add("Bad car");
//		subjectNames.add("好车");
//		SparkSearchIndexTable sparkSearchIndexTable = new SparkSearchIndexTable(tableName,indexTableName);
//		sparkSearchIndexTable.searchIndexTable(namedEntityName, subjectNames);
		
		String tableName = "testCop_10000_2";
    	HTableInterface hTable = null;
    	try {
			 hTable = HbaseTool.getInstance().getTable(tableName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	try {
			if(!HbaseTool.getAdmin().tableExists(tableName)){
				String[] families = {"objects","attributes","array_objects","index"};
				HbaseTool.createTable(tableName,families);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	//添加小部分数据进行试验
//		try {
//			HbaseTool.getInstance().putTable();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		HbaseTool.getInstance().buildIndex();
		
		SearchIndexTableByCoprocessor si = new SearchIndexTableByCoprocessor();
		String nameEntity = "zhu";
		List<String> subjectNames = new ArrayList<String>();
		//subjectNames.add("test");
		List<EntityInfo> list = si.search(nameEntity, subjectNames);
		for(EntityInfo en:list){
			System.out.println(en.getNm());
		}
	}

}
