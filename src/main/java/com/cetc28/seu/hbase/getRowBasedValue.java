package com.cetc28.seu.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

public class getRowBasedValue {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		HTableInterface hTable = null;
		try {
			hTable = HbaseTool.getInstance().getTable(HbaseConfig.tableName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Scan scan = new Scan();
		
		SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("attributes"),
				Bytes.toBytes("bdnm"), CompareOp.EQUAL, Bytes.toBytes("Rose"));
		columnValueFilter.setFilterIfMissing(true);		
		scan.setFilter(columnValueFilter);
		ResultScanner rs = null;
		try {
			 rs = hTable.getScanner(scan);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for(Result result : rs){
			System.out.println(Bytes.toString(result.getRow()));
		}
	}

}
