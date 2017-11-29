package com.cetc28.seu.hbase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import com.cetc28.seu.hbase.IndexTable.SparkSearchIndexTable;

public class OldTestHbase {
	public static void main(String[] args) throws IOException {
		/*String tableName = "experiment";
		String indexTableName = "index5";
    	List<String> list = new ArrayList<String>();
		list.add("Bad car");
		list.add("Good car");
		list.add("好车");
		SparkBuildIndexTable sparkBuildIndexTable = new SparkBuildIndexTable(tableName,indexTableName);
		try {
			sparkBuildIndexTable.buildTable(list);
		} catch (IOException e) {
			e.printStackTrace();
		}*/
/*		final String indexTableName = "index5";
		final String tableName = "experiment";
			
		String namedEntityName = "Audi";
		List<String> subjectNames = new ArrayList<String>();
		subjectNames.add("Good car");
		subjectNames.add("Bad car");
		subjectNames.add("好车");
		//long start = System.currentTimeMillis();
		SparkSearchIndexTable sparkSearchIndexTable = new SparkSearchIndexTable(tableName,indexTableName);
		List<EntityInfo> entityList = new ArrayList<EntityInfo>();
		try {
			entityList = sparkSearchIndexTable.searchIndexTable(namedEntityName, subjectNames);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		entityList.size();
		//long end = System.currentTimeMillis();
		//System.out.println("pay time: " + (end - start));
		for(EntityInfo entityInfo : entityList)
		{
			System.out.println("entityList: " +entityInfo.getSubjectName()+entityInfo.getDetails());
		}*/
		
		final String indexTableName = "indexHbaseTest40000";
		final String tableName = "HbaseTest40000";
			
		String namedEntityName = "zhu10000";
		List<String> subjectNames = new ArrayList<String>();
		subjectNames.add("Good car");
		subjectNames.add("Bad car");
		subjectNames.add("好车");
		
		SparkSearchIndexTable sparkSearchIndexTable = new SparkSearchIndexTable(tableName,indexTableName);

		long start = System.currentTimeMillis();
		sparkSearchIndexTable.searchIndexTable(namedEntityName, subjectNames);
		long end = System.currentTimeMillis();
		System.out.println("pay time: " + (end - start));
		
		long start1 = System.currentTimeMillis();
		String namedEntityName1 = "zhu10000";
		sparkSearchIndexTable.searchIndexTable(namedEntityName1, subjectNames);
		long end1= System.currentTimeMillis();
		System.out.println("pay time2222: " + (end1- start1));
	}
}