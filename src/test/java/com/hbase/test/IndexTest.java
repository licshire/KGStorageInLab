package com.hbase.test;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.cetc28.seu.hbase.HbaseConfig;
import com.cetc28.seu.hbase.HbaseTool;

public class IndexTest {
	private static HbaseTool hbaseTool = HbaseTool.getInstance();
	public static void main(String[] args) throws IOException
	{
		HTableInterface htable = hbaseTool.getTable(HbaseConfig.tableName);
		Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("zhu0"));
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner rs = htable.getScanner(scan);
		for(Result result : rs){
			System.out.println(Bytes.toString(result.getRow()));
		}

		
	}
}
