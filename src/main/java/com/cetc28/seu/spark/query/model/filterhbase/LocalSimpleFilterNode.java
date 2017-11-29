package com.cetc28.seu.spark.query.model.filterhbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import com.cetc28.seu.hbase.HbaseTool;
import com.cetc28.seu.query.struct.QueryCondition;
import com.cetc28.seu.rdf.Term;
import com.cetc28.seu.spark.query.model.LeafNode;
import com.cetc28.seu.spark.query.model.QueryNode;
import com.cetc28.seu.spark.query.model.simple.SimpleFilterNode;
import com.cetc28.seu.spark.query.result.LocalResultSet;
import com.cetc28.seu.spark.query.result.ResultSet;
import com.cetc28.seu.sparql.FilterClause;

public class LocalSimpleFilterNode extends LeafNode{

	private static final long serialVersionUID = 5600695106350071907L;
	public HbaseTool hbaseTool = HbaseTool.getInstance();
	public static String tableName = "HbaseRegion2";
	public static List<FilterClause<?>> filterClause;
	
	public LocalSimpleFilterNode(QueryCondition attributes) {
		this.setAttributes(attributes);
	}

	public LocalSimpleFilterNode(QueryCondition attributes, Configuration conf, JavaSparkContext sc) {
		this.setAttributes(attributes);
		QueryNode.hbaseConf = conf; 
		QueryNode.sc = sc;
	}

	public LocalSimpleFilterNode(int id, int lchild, int rchild, int parent, Term term, QueryCondition attributes) {
		super(id, lchild, rchild, parent, term);
		this.setAttributes(attributes);
	}

	// 加入filter函数
	public LocalSimpleFilterNode(int id, int lchild, int rchild, int parent, Term term, QueryCondition attributes,
			List<FilterClause<?>> filterClause) {
		super(id, lchild, rchild, parent, term);
		this.setAttributes(attributes);
		LocalSimpleFilterNode.filterClause = filterClause;
	}
	
	

	public ResultSet query() {
		//TODO set return columns
		String family = this.getAttributes().getFamily();
		Map<String, String> attributeMap = this.getAttributes().getConditions();
		List<Result> list = new ArrayList<Result>();
		LocalResultSet lResultSet = new LocalResultSet();
		List<Filter> fList = new ArrayList<>();
		if (!attributeMap.isEmpty()) {
			for (Map.Entry<String, String> entry : attributeMap.entrySet()) {

				SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter(family.getBytes(),
						entry.getKey().getBytes(), CompareOp.EQUAL, entry.getValue().getBytes());
				columnValueFilter.setFilterIfMissing(true);
				fList.add(columnValueFilter);
			}
		}
		FilterList filterList = new FilterList(fList);
		Scan scan = new Scan();
		scan.setFilter(filterList);
		scan.setCaching(1000);
		scan.setCacheBlocks(false);
		HTableInterface hTable = null;
		try {
			 hTable = hbaseTool.getTable(tableName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ResultScanner resultScanner = null;
		try {
			 resultScanner = hTable.getScanner(scan);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		for(Result result : resultScanner)
		{
			list.add(result);
		}
		lResultSet.setResults(list);
		resultScanner.close();
		return lResultSet;

	}

}
