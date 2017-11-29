package com.cetc28.seu.spark.query.model;


import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.spark.api.java.JavaSparkContext;

import com.cetc28.seu.rdf.Term;
import com.cetc28.seu.spark.query.result.ResultSet;



public abstract class QueryNode implements Serializable{

	private static final long serialVersionUID = -2961214832101500548L;
	private int id;
	private int lchild;
	private int rchild;
	private int parent;
	private Term term;//term 
	protected static Scan scan=new Scan();
	public static Configuration hbaseConf;
	public static JavaSparkContext sc;
	public static List<QueryNode> trees=new ArrayList<>();
	public QueryNode(){
		id=-1;
		lchild=-1;
		rchild=-1;
		parent=-1;
	}
	
	public QueryNode(int id, int lchild, int rchild, int parent, Term term) {

		this.id = id;
		this.lchild = lchild;
		this.rchild = rchild;
		this.parent = parent;
		this.term = term;
	}

	public abstract ResultSet query();
	public int getLchild() {
		return lchild;
	}
	public void setLchild(int lchild) {
		this.lchild = lchild;
	}
	public int getRchild() {
		return rchild;
	}
	public void setRchild(int rchild) {
		this.rchild = rchild;
	}
	public int getParent() {
		return parent;
	}

	public void setParent(int parent) {
		this.parent = parent;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
	
	public Term getTerm() {
		return term;
	}
	public void setTerm(Term term) {
		this.term = term;
	}
	
	public void setScanToConf(Scan scan) {
		try {
			ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
			String scanToString = Base64.encodeBytes(proto.toByteArray());
			hbaseConf.set(TableInputFormat.SCAN, scanToString);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
