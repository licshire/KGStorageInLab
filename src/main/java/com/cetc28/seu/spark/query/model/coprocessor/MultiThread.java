package com.cetc28.seu.spark.query.model.coprocessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyFinal.Answer;
import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyFinal.AnswerInfo;
import com.google.protobuf.ByteString;


/**
 * TODO 根据行键多线程查找
 * @author Think
 *
 */
public class MultiThread implements Callable<ByteString> {
	public String rowKey;
	public HRegion region;
	public String family;
	public List<String> columnList;
	public List<String> valueList;
	public List<String> subjectList;
	private static final Log LOG = LogFactory.getLog(MultiThread.class);

	/**
	 * 根据region,行键初始化
	 * @param row
	 * @param region
	 */
	public MultiThread(String row, HRegion region){
		this.rowKey = row;
		this.region = region;
		this.columnList = new ArrayList<String>();
		this.valueList = new ArrayList<String>();
		this.subjectList = new ArrayList<String>();
	}
	
	/**
	 * 根据region,行键，主题初始化
	 * @param row
	 * @param region
	 * @param subjectList
	 */
	public MultiThread(String row, HRegion region, List<String> subjectList){
		this.rowKey = row;
		this.region = region;
		this.columnList = new ArrayList<String>();
		this.valueList = new ArrayList<String>();
		this.subjectList = subjectList;
	}
	
	/**
	 * 根据region，行键，列表，值表初始化
	 * @param row
	 * @param region
	 * @param columnList
	 * @param valueList
	 */
	public MultiThread(String row, HRegion region, List<String> columnList, List<String> valueList){
		this.rowKey = row;
		this.region = region;
		this.columnList = columnList;
		this.valueList = valueList;
		this.subjectList = new ArrayList<String>();
	}
	
	/**
	 * 根据行键，region,family,列表，值表进行初始化
	 * @param row
	 * @param region
	 * @param family
	 * @param columnList
	 * @param valueList
	 */
	public MultiThread(String row, HRegion region, String family, List<String> columnList, List<String> valueList){
		this.rowKey = row;
		this.region = region;
		this.family = family;
		this.columnList = columnList;
		this.valueList = valueList;
		this.subjectList = new ArrayList<String>();
	}
	
//	public MultiThread(String row){
//		this.rowKey = row;
//	}
	
	@Override
	public ByteString call() throws Exception {
		Get get = new Get(Bytes.toBytes(rowKey));
		if(columnList.size() != 0 && valueList.size() != 0){
			List<Filter> l = new ArrayList<Filter>();
			for(int i = 0; i < columnList.size(); i++){
				SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(family),Bytes.toBytes(columnList.get(i)),CompareOp.EQUAL,Bytes.toBytes(valueList.get(i)));
				filter.setFilterIfMissing(true);
				l.add(filter);
			}
			FilterList f = new FilterList(l);
			get.setFilter(f);
		}
		Result result = region.get(get);
		String row = Bytes.toString(result.getRow());
//		LOG.info("MultiTread row:=============" + row);
		if(subjectList.size() != 0){
			for(String subject : subjectList){
//				LOG.info("subject:=============" + subject);
				//模糊搜索主题
				if(row.contains(subject)){
//					LOG.info("TestMultiThread:=============" + row);
					return resAnswer(result, row);
				}
			}
			return Answer.newBuilder().build().toByteString();
		}else{
			return resAnswer(result, row);
		}
	}

	/**
	 * 根据result和行键封装结果集
	 * @param result
	 * @param row
	 * @return
	 */
	public ByteString resAnswer(Result result, String row){
		Answer.Builder answer = Answer.newBuilder();
		answer.setRow(row);
		for(Cell cell : result.rawCells()){
			String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
			String column =  Bytes.toString(CellUtil.cloneQualifier(cell));
			String value = Bytes.toString(CellUtil.cloneValue(cell));
			AnswerInfo.Builder ai = AnswerInfo.newBuilder();
			ai.setColumnFamily(columnFamily); 
			ai.setColumn(column);
			ai.setValue(value);
			answer.addAi(ai);
		}
		Answer res = answer.build();
		ByteString r = res.toByteString();
		return r;
	}
}
