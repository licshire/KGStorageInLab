package com.cetc28.seu.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;

import com.cetc28.seu.hbase.IndexTable.SearchIndexTableByCoprocessor;
import com.cetc28.seu.loading.theme.model.EntityInfo;

public class OldTestKeyWord {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String tableName = HbaseConfig.tableName;

    	try {
			if(!HbaseTool.getAdmin().tableExists(tableName)){
				String[] families = {"objects","attributes","array_objects","index"};
				HbaseTool.createTable(tableName,families);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	HTableInterface hTable = null;
    	try {
			 hTable = HbaseTool.getInstance().getTable(tableName);
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
		Long start = System.currentTimeMillis();
		SearchIndexTableByCoprocessor si = new SearchIndexTableByCoprocessor();
		String nameEntity = "Benz";
		List<String> subjectNames = new ArrayList<String>();
		//subjectNames.add("test");
		List<EntityInfo> list = si.search(nameEntity, subjectNames);
		Long end = System.currentTimeMillis();
		System.out.println("pay time: " + (end - start));
		for(EntityInfo en:list){
			System.out.println(en.getNm());
		}
	}

}
