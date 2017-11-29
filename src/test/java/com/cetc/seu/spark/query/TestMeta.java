package com.cetc.seu.spark.query;

import java.io.IOException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.cetc28.seu.hbase.HbaseConfig;
import com.cetc28.seu.hbase.HbaseTool;

public class TestMeta {

	public static void main(String[] args) {

		String tableName = HbaseConfig.tableName;
		try {
			if (!HbaseTool.getAdmin().tableExists(tableName)) {
				String[] families = { "objects", "attributes", "array_objects", "index" };
				HbaseTool.createTable(tableName, families, 10);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			HTableInterface htable = HbaseTool.getInstance().getTable("hbase:meta");
			Scan scan = new Scan();
			ResultScanner rss = htable.getScanner(scan);
			for(Result rs : rss){
				System.out.println(Bytes.toString(rs.getRow()));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
