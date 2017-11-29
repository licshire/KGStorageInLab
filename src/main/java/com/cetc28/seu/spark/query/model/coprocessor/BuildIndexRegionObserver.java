package com.cetc28.seu.spark.query.model.coprocessor;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import com.cetc28.seu.hbase.HbaseConfig;

public class BuildIndexRegionObserver extends BaseRegionObserver {
	private static final Log LOG = LogFactory.getLog(BuildIndexRegionObserver.class);

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		super.stop(e);
	}

	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
			throws IOException {
		super.postPut(e, put, edit, durability);
		LOG.info("postPut==================================================");
		// LOG.info(e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString()+"===========");
		// LOG.info(HbaseConfig.tableName+"======================");
		if (!e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString()
				.equals(HbaseConfig.tableName)) {
			return;
		}
		String startIndex = Bytes.toString(e.getEnvironment().getRegion().getStartKey());
		String rowKey = Bytes.toString(put.getRow());
		if (rowKey.equals("9999a")) {
//			LOG.info("LastpostPut==================================================");
//			Put putLast = new Put(Bytes.toBytes("999aa"));
//			int i = 0;
//			for (String str : indexHashSet) {
//				// 放入表中的region为9999a的部分
//				LOG.info("str==================================================" + str);
//				put.add(Bytes.toBytes("attributes"), Bytes.toBytes(String.valueOf(i)), Bytes.toBytes(str));
//				i++;
//			}
//			e.getEnvironment().getRegion().put(putLast);
//			LOG.info("LastpostPutFinish==================================================");
//			e.getEnvironment().getRegion().flushcache();
			return;
		}
		for (Entry<byte[], List<Cell>> entry : put.getFamilyCellMap().entrySet()) {
			for (Cell cell : entry.getValue()) {
				String family = Bytes.toString(CellUtil.cloneFamily(cell));
				String column = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				// 是否为纯数字
				String s1 = "^-?\\d+$";
				// 是否为时间
				String s2 = "(\\d{1,4}[-|\\/|年|\\.]\\d{1,2}[-|\\/|月|\\.]\\d{1,2}([日|号])?(\\s)*(\\d{1,2}([点|时])?((:)?\\d{1,2}(分)?((:)?\\d{1,2}(秒)?)?)?)?(\\s)*(PM|AM)?)";
				// 字段为bdnm,或包含id
				if ((family.equals("attributes") && column.equals("bdnm"))
						|| (family.equals("attributes") && column.contains("id"))) {
					// 对建立索引的列存放列名
					// LOG.info("Index rowKey========="
					// +startIndex+":"+value+":"+family+":"+column+"_"+rowKey);
					Put putNew = new Put(
							Bytes.toBytes(startIndex + ":" + value + ":" + family + ":" + column + "_" + rowKey));
					putNew.add(Bytes.toBytes("index"), Bytes.toBytes("1"), Bytes.toBytes(rowKey));
					e.getEnvironment().getRegion().put(putNew);
					// e.getEnvironment().getRegion().flushcache();
				} else if (value.matches(s1) || value.matches(s2) || value.equals("null") || value.equals("Null")
						|| (family.equals("attributes") && column.equals("data")) || family.equals("index")) {
					continue;
				} else {
					Put put2 = new Put(
							Bytes.toBytes(startIndex + ":" + value + ":" + family + ":" + column + "_" + rowKey));
					put2.add(Bytes.toBytes("index"), Bytes.toBytes("1"), Bytes.toBytes(rowKey));
					e.getEnvironment().getRegion().put(put2);
					// e.getEnvironment().getRegion().flushcache();

				}
			}
		}
		// LOG.info("FINISH======================================================");
	}

}
