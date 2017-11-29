
package com.cetc28.seu.spark.query.model.simple;



import java.util.ArrayList;

import java.util.List;
import java.util.Map;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;


import com.cetc28.seu.query.struct.QueryCondition;
import com.cetc28.seu.rdf.Term;
import com.cetc28.seu.spark.query.model.LeafNode;
import com.cetc28.seu.spark.query.model.QueryNode;
import com.cetc28.seu.spark.query.result.DistributedResultSet;
import com.cetc28.seu.sparql.FilterClause;

import scala.Tuple2;



public class SimpleFilterNode extends LeafNode {

	private static final long serialVersionUID = 5600695106350071907L;

	public SimpleFilterNode(QueryCondition attributes){
		this.setAttributes(attributes);
	}
	public SimpleFilterNode(QueryCondition attributes,Configuration conf,JavaSparkContext sc) {
		this.setAttributes(attributes);		
		QueryNode.hbaseConf=conf;
		QueryNode.sc=sc;
	}
	
	public SimpleFilterNode(int id, int lchild, int rchild, int parent, Term term,QueryCondition attributes) {
		super(id, lchild, rchild, parent, term);
		this.setAttributes(attributes);	
	}
	//加入filter函数
	public SimpleFilterNode(int id, int lchild, int rchild, int parent, Term term,QueryCondition attributes, List<FilterClause<?>> filterClause) {
		super(id, lchild, rchild, parent, term);
		this.setAttributes(attributes);	
	}
	
/*	public List<Filter> filterQuery(List<FilterClause> filterClause,List<Filter> fList)
	{
		if(filterClause != null && filterClause.size() != 0)
		{
			for(FilterClause localFilterClause : filterClause)
			{
				//判断filter条件主语与三元组主语是否相符
				if(this.getTerm().equals(localFilterClause.getTerm()))
				{
					switch(localFilterClause.getSymbol())
					{
						case ">":
							Filter filterG = new RowFilter(CompareOp.GREATER,new BinaryComparator(Bytes.toBytes(localFilterClause.getValue())));
							fList.add(filterG);
							break;
						case "<":
							Filter filterL = new RowFilter(CompareOp.LESS,new BinaryComparator(Bytes.toBytes(localFilterClause.getValue())));
							fList.add(filterL);
							break;
						case "=":
							Filter filterE = new RowFilter(CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes(localFilterClause.getValue())));
							fList.add(filterE);
							break;
						case ">=":
							Filter filterGOE = new RowFilter(CompareOp.GREATER_OR_EQUAL,new BinaryComparator(Bytes.toBytes(localFilterClause.getValue())));
							fList.add(filterGOE);
							break;
						case "<=":
							Filter filterLOE = new RowFilter(CompareOp.LESS_OR_EQUAL,new BinaryComparator(Bytes.toBytes(localFilterClause.getValue())));
							fList.add(filterLOE);
							break;
						case "!=":
							Filter filterNE = new RowFilter(CompareOp.NOT_EQUAL,new BinaryComparator(Bytes.toBytes(localFilterClause.getValue())));
							fList.add(filterNE);
							break;
					}
				}
				
			}
		}
		return fList;
	}*/
	
	public DistributedResultSet query() {
		
		
		String family = this.getAttributes().getFamily();
		
		List<String> answers = this.getAttributes().getAnswer();
		DistributedResultSet rs=new DistributedResultSet();
		rs.setAskColumns(answers);
		
		Map<String, String> attributeMap = this.getAttributes().getConditions();

		List<Filter> fList = new ArrayList<>();
		if (!attributeMap.isEmpty()){
            for (Map.Entry<String, String> entry : attributeMap.entrySet()) {
				
				SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter(family.getBytes(),
						entry.getKey().getBytes(), CompareOp.EQUAL, entry.getValue().getBytes());
				columnValueFilter.setFilterIfMissing(true);
				fList.add(columnValueFilter);
			}
		}
			
		//添加的filter函数
//		List<Filter> listFilter = filterQuery(SimpleFilterNode.filterClause,fList);
//		FilterList filterList = new FilterList(listFilter);
//		scan.setFilter(filterList);
		scan.setCaching(1000);//cache
		setScanToConf(scan);
		JavaPairRDD<ImmutableBytesWritable, Result> rdd = sc.newAPIHadoopRDD(hbaseConf,
				TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
		
		JavaPairRDD<String,Result> rddResult=rdd.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable,Result>,String, Result>() {

			private static final long serialVersionUID = -842442285360302812L;

			@Override
			public Tuple2<String, Result> call(Tuple2<ImmutableBytesWritable, Result> t)
					throws Exception {
				byte[] key=t._2.getRow();
				Tuple2<String, Result> result=new Tuple2<String, Result>(Bytes.toString(key), t._2);
				return result;
			}
			
		});
		rs.setResultRDD(rddResult);

		
		return rs;
	}
	
	

	
	
}
