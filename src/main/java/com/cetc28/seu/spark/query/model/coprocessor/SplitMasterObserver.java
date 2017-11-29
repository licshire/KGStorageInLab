package com.cetc28.seu.spark.query.model.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.cetc28.seu.hbase.HbaseConfig;

public class SplitMasterObserver extends BaseRegionObserver{
	private static final Log LOG = LogFactory.getLog(SplitMasterObserver.class);

	
	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		super.start(e);
	}

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		super.stop(e);
	}

	@Override
	public void postSplit(ObserverContext<RegionCoprocessorEnvironment> e, HRegion l, HRegion r) throws IOException {
		//super.postSplit(e, l, r);
		//对于源split的操作，先删除旧的索引，再建新的索引
		if(!l.getRegionInfo().getTable().getNameAsString().equals(HbaseConfig.tableName) && !r.getRegionInfo().getTable().getNameAsString().equals(HbaseConfig.tableName)){
			return;
		}
		LOG.info("split finish ====================================");
		LOG.info("left: ===========" +l.getRegionInfo().toString());
		LOG.info("right:==========" + r.getRegionInfo().toString());
		LOG.info("coprocessor e compact?:============== " + e.getEnvironment().getRegion().needsCompaction());
		LOG.info("region l compact?: ==========" + l.needsCompaction());
		LOG.info("region r compact?: ============" + r.needsCompaction());

		//对于新的split的操作
		buildIndex(e,r);
//		r.flushcache();

		deleteIndex(e,l);
		buildIndex(e,l);		
	}

	//TODO 删除左region的索引
	public void deleteIndex(ObserverContext<RegionCoprocessorEnvironment> c, HRegion h){
		Scan scan = new Scan();
		Filter filter = new FirstKeyOnlyFilter();
		String start = Bytes.toString(h.getStartKey())+":";
		scan.setStartRow(Bytes.toBytes(start));
		String end = Bytes.toString(h.getStartKey())+";";
		scan.setStopRow(Bytes.toBytes(end));
		scan.addFamily(Bytes.toBytes("index"));
		scan.setFilter(filter);
		LOG.info("start delete==================");
		try {
			InternalScanner rs = h.getScanner(scan);
//			List<Delete> deletes = new ArrayList<Delete>();
			List<Cell> results = new ArrayList<>();
			boolean hasMore = false;
			do{
				hasMore = rs.next(results);
				if(results.size() == 0)
				{
					break;
				}
				Delete delete = new Delete(CellUtil.cloneRow(results.get(0)));
//				deletes.add(delete);
				h.delete(delete);
			}while(hasMore);
			h.flushcache();
			rs.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		LOG.info("successful delete====================");
	}
	
	//TODO 新建索引
	public void buildIndex(ObserverContext<RegionCoprocessorEnvironment> c,HRegion region){
		LOG.info("start rebuild ====================================");
//		HTableInterface htable = null;
//		try {
//			htable = c.getEnvironment().getTable(TableName.valueOf(HbaseConfig.tableName));
//		} catch (IOException e1) {
//			e1.printStackTrace();
//		}
		String start = Bytes.toString(region.getStartKey());
		String end = Bytes.toString(region.getEndKey());
		Scan scan = new Scan();
		scan.setStartRow(region.getStartKey());
		scan.setStopRow(region.getEndKey());
		InternalScanner resultScanner = null;
		try {
			resultScanner = region.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}
		List<Cell> result = new ArrayList<>();
		try {
			boolean hasMore = false;
			do{
				hasMore = resultScanner.next(result);
				String dataIndex = Bytes.toString(CellUtil.cloneRow(result.get(0)));
				LOG.info("dataIndex========" + dataIndex);
				String regionNumber = start;
				ArrayList<Put> arrayList = new ArrayList<Put>();
				for(Cell cell : result)
				{
					String family = Bytes.toString(CellUtil.cloneFamily(cell));
					String column = Bytes.toString(CellUtil.cloneQualifier(cell));
					String value = Bytes.toString(CellUtil.cloneValue(cell));
					//是否为纯数字
					String s1 = "^-?\\d+$";
					//是否为时间
					String s2 = "(\\d{1,4}[-|\\/|年|\\.]\\d{1,2}[-|\\/|月|\\.]\\d{1,2}([日|号])?(\\s)*(\\d{1,2}([点|时])?((:)?\\d{1,2}(分)?((:)?\\d{1,2}(秒)?)?)?)?(\\s)*(PM|AM)?)";
					//字段为bdnm,或包含id
					if((family.equals("attributes") && column.equals("bdnm")) || (family.equals("attributes") && column.contains("id"))){
						Put put = new Put(Bytes.toBytes(regionNumber+":"+value+":"+family+":"+column+"_"+dataIndex));
						put.add(Bytes.toBytes("index"), Bytes.toBytes("1"), Bytes.toBytes(dataIndex));
						arrayList.add(put);
					}else if(value.matches(s1) || value.matches(s2) || value.equals("null") || value.equals("Null") || (family.equals("attributes") && column.equals("data")) || family.equals("index")){
						continue;
					}else{	
						Put put = new Put(Bytes.toBytes(regionNumber+":"+value+":"+family+":"+column+"_"+dataIndex));
						put.add(Bytes.toBytes("index"), Bytes.toBytes("1"), Bytes.toBytes(dataIndex));
						arrayList.add(put);
					}
					try {
						LOG.info("11111================c");
						for(Put put : arrayList)
							region.put(put);
						LOG.info("222222222222================c");
//						region.flushcache();
					} catch (IOException e) {
						e.printStackTrace();
					}	
				}
			}while(hasMore);
			resultScanner.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		LOG.info("success rebuild ====================================");
	}
}
