package com.cetc28.seu.hbase;

import java.io.IOException;

public class CopyTable {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		copyData();
	}
	//备份数据
	public static void copyData(){
		String tableName = HbaseConfig.copyTableName;
    	try {
			if(!HbaseTool.getAdmin().tableExists(tableName)){
				String[] families = {"objects","attributes","array_objects","index"};
				HbaseTool.createTable(tableName,families);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
