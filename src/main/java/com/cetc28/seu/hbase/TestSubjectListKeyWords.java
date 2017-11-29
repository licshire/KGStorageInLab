package com.cetc28.seu.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import com.cetc28.seu.spark.query.model.coprocessor.SearchSubjectKeyWords;
import com.cetc28.seu.spark.query.result.ResultSet;

/**
 * TODO 主题+关键字搜索
 * @author Think
 *
 */
public class TestSubjectListKeyWords {

	public static void main(String[] args) {
		String tableName = HbaseConfig.tableName;

    	try {
			if(!HbaseTool.getAdmin().tableExists(tableName)){
				String[] families = {"objects","attributes","array_objects","index"};
				HbaseTool.createTable(tableName,families);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    	HTableInterface hTable = null;
    	try {
			 hTable = HbaseTool.getInstance().getTable(tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
		SearchSubjectKeyWords ssk = new SearchSubjectKeyWords();
		List<String> nameList = new ArrayList<String>();
		String nameEntity = "zhu0";
		nameList.add(nameEntity);
		List<String> subjectNames = new ArrayList<String>();
		subjectNames.add("test");
		ResultSet result = ssk.getSubjectAKeyWords(nameList, subjectNames);
		result.show(100);
	}

}
