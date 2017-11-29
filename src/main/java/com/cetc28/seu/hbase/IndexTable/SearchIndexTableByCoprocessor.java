package com.cetc28.seu.hbase.IndexTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import com.cetc28.seu.hbase.HbaseConfig;
import com.cetc28.seu.hbase.HbaseTool;
import com.cetc28.seu.loading.theme.model.EntityInfo;
/**
 * 关键字搜索
 * @author Think
 *
 */
public class SearchIndexTableByCoprocessor {
	public String tableName = HbaseConfig.tableName;
	
	public List<EntityInfo> search(String namedEntityName, List<String> subjectNames){
		List<EntityInfo> list = new ArrayList<EntityInfo>();
		List<Row> batch = new ArrayList<Row>();		
//		List<String> resultList = HbaseTool.getInstance().startSearchIndex("", "", namedEntityName);
		List<String> resultList = null;
		
		//从协处理器中查出值
		Map<String,String> hashMap = new HashMap<String,String>();
		
		if(subjectNames == null || subjectNames.size()== 0 ||subjectNames.isEmpty()){
			for(String result : resultList){
				//result格式0000::topic:id
				Get get = new Get(Bytes.toBytes(result));
				batch.add(get);
			}
		}else{
			for(String subjectName : subjectNames){
				hashMap.put(subjectName, subjectName);
			}
			
			for(String result : resultList){
				//result格式0000::topic:id
				String[] topic = result.split(":");
				if(hashMap.containsKey(topic[1])){
					Get get = new Get(Bytes.toBytes(result));
					batch.add(get);
				}
			}	
		}
		
		//批处理获得值
		Object[] results = new Object[batch.size()];
		try {
			HbaseTool.getInstance().getTable(tableName).batch(batch, results);
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for(Object object : results){
			Result result = (Result)object;
			String rowKey = Bytes.toString(result.getRow());
			String[] topic = rowKey.split(":");
			Map<String,String> map = new HashMap<String,String>();
			
			EntityInfo entityInfo = new EntityInfo();
			entityInfo.setSubjectName(topic[1]);
			entityInfo.setNm(rowKey);
			for(Cell cell : result.rawCells())
			{
				map.put(Bytes.toString(CellUtil.cloneFamily(cell))+":"+Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
			}
			entityInfo.setDetails(map);
			list.add(entityInfo);
		}
		return list;
	}
}
