package com.cetc28.seu.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class TestIndexIncrement {

	public static void main(String[] args) {
		
		try {
			if(!HbaseTool.getAdmin().tableExists(HbaseConfig.tableName)){
				String[] families = {"objects","attributes","array_objects","index"};
				HbaseTool.createTable(HbaseConfig.tableName,families);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String prefix = HbaseTool.getInstance().getRandomPrefix();
		String dataIndex = prefix+":judge:"+prefix.hashCode()+":"+100;
		
		Put put = new Put(Bytes.toBytes(dataIndex));	    	
    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("Jack"));
    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu0"));
		put.add(Bytes.toBytes("attributes"), Bytes.toBytes("age"),Bytes.toBytes(100+""));
		try {
			HbaseTool.getInstance().getTable(HbaseConfig.tableName).put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		addIndexIncrement(put);
	}
	public static void addIndexIncrement(Put putData){
		String dataIndex = Bytes.toString(putData.getRow());
		String dataIndexTemp = dataIndex.substring(0, 1);
		String regionNumber = HbaseTool.getInstance().getRegionNumber(dataIndexTemp);
		System.out.println(dataIndex);
		
		for(Entry<byte[], List<Cell>> entry : putData.getFamilyCellMap().entrySet()){
			ArrayList<Put> arrayList = new ArrayList<Put>();
			for(Cell cell : entry.getValue()){
				String family = Bytes.toString(CellUtil.cloneFamily(cell));
				String column = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				System.out.println(family +"　"+column+" "+value);
				
				//是否为纯数字
				String s1 = "^-?\\d+$";
				//是否为时间
				String s2 = "(\\d{1,4}[-|\\/|年|\\.]\\d{1,2}[-|\\/|月|\\.]\\d{1,2}([日|号])?(\\s)*(\\d{1,2}([点|时])?((:)?\\d{1,2}(分)?((:)?\\d{1,2}(秒)?)?)?)?(\\s)*(PM|AM)?)";
				//字段为bdnm,或包含id
				if((family.equals("attributes") && column.equals("bdnm")) || (family.equals("attributes") && column.contains("id"))){
					Put put = new Put(Bytes.toBytes(regionNumber+":"+value+":"+family+":"+column+"_"+dataIndex));
					put.add(Bytes.toBytes("index"), Bytes.toBytes("1"), Bytes.toBytes(dataIndex));
					arrayList.add(put);
				}else if(value.matches(s1) || value.matches(s2) || value.equals("Null") || (family.equals("attributes") && column.equals("data"))){
					continue;
				}else{
					Put put = new Put(Bytes.toBytes(regionNumber+":"+value+":"+family+":"+column+"_"+dataIndex));
					put.add(Bytes.toBytes("index"), Bytes.toBytes("1"), Bytes.toBytes(dataIndex));
					arrayList.add(put);
				}
				try {
					HbaseTool.getInstance().getTable(HbaseConfig.tableName).put(arrayList);
					HbaseTool.getInstance().getTable(HbaseConfig.tableName).flushCommits();

				} catch (IOException e) {
				// TODO Auto-generated catch block
					e.printStackTrace();
				}	
			}
		}
	}
}
