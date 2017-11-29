
package com.cetc28.seu.spark.query.model.memory;

import java.util.ArrayList;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import com.cetc28.seu.query.struct.QueryCondition;
import com.cetc28.seu.rdf.Term;
import com.cetc28.seu.spark.query.model.LeafNode;
import com.cetc28.seu.spark.query.model.QueryNode;
import com.cetc28.seu.spark.query.model.simple.SimpleFilterNode;
import com.cetc28.seu.spark.query.result.DistributedResultSet;
import com.cetc28.seu.sparql.FilterClause;

import scala.Tuple2;

public class SimpleFilterInMemoryNode extends LeafNode {

	private static final long serialVersionUID = 5600695106350071907L;
	public static JavaPairRDD<ImmutableBytesWritable, Result> rdd;

	public SimpleFilterInMemoryNode(QueryCondition attributes) {
		this.setAttributes(attributes);
	}

	public SimpleFilterInMemoryNode(QueryCondition attributes, Configuration conf, JavaSparkContext sc) {
		this.setAttributes(attributes);
		QueryNode.hbaseConf = conf;
		QueryNode.sc = sc;
	}

	public SimpleFilterInMemoryNode(int id, int lchild, int rchild, int parent, Term term, QueryCondition attributes) {
		super(id, lchild, rchild, parent, term);
		this.setAttributes(attributes);
	}

	// 加入filter函数
	public SimpleFilterInMemoryNode(int id, int lchild, int rchild, int parent, Term term, QueryCondition attributes,
			List<FilterClause<?>> filterClause) {
		super(id, lchild, rchild, parent, term);
		this.setAttributes(attributes);
	}

	public DistributedResultSet query() {
		// TODO set return columns
		long start = System.currentTimeMillis();
		final String family = this.getAttributes().getFamily();
//		System.out.println("family: " + family);
		List<String> answers = this.getAttributes().getAnswer();
		final Map<String, String> attributeMap = this.getAttributes().getConditions();
//		for (Map.Entry<String, String> entry : attributeMap.entrySet()) {
//			System.out.println(entry.getKey()+" " +entry.getValue());
//		}

		JavaPairRDD<ImmutableBytesWritable, Result> result = rdd
				.filter(new Function<Tuple2<ImmutableBytesWritable, Result>, Boolean>() {

					/**
					 * 过滤简单filter实现
					 */
					private static final long serialVersionUID = -7221510777727811274L;

					@Override
					public Boolean call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception { // TODO
																										// Auto-generated
																										// method
						if (!attributeMap.isEmpty() && attributeMap.size() != 0) {
							for (Map.Entry<String, String> entry : attributeMap.entrySet()) {
								byte[] value = v1._2.getValue(Bytes.toBytes(family), Bytes.toBytes(entry.getKey()));
								if (Bytes.toString(value) == null || !Bytes.toString(value).equals(entry.getValue())) {
									return false;
								}

							}
							return true;

						} else {
							return false;
						}
					}

				});

		JavaPairRDD<String, Result> rddResult = result
				.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Result>() {

					private static final long serialVersionUID = -842442285360302812L;

					@Override
					public Tuple2<String, Result> call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
						byte[] key = t._2.getRow();
						Tuple2<String, Result> result = new Tuple2<String, Result>(Bytes.toString(key), t._2);
						return result;
					}

				});

		DistributedResultSet rs = new DistributedResultSet();
		rs.setResultRDD(rddResult);
		// rddResult.count();
		// long end = System.currentTimeMillis();
		// System.out.println("simple time: " + (end - start));
		// System.out.println("simple:");
		return rs;
	}

}
