package com.cetc28.seu.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.cetc28.seu.spark.query.model.coprocessor.MultiThread;
import com.google.protobuf.ByteString;

public class CustomKeyPrefixRegionSplitPolicy extends ConstantSizeRegionSplitPolicy {
	private static final Log LOG = LogFactory.getLog(CustomKeyPrefixRegionSplitPolicy.class);

	@Override
	protected void configureForRegion(HRegion region) {
		super.configureForRegion(region);
	}

	// 需要修改split策略，发现索引为有效数据/2，split
	@Override
	protected byte[] getSplitPoint() {
		int prefixLength = 4;
		byte[] splitPoint = super.getSplitPoint();
		String splitTemp = Bytes.toString(splitPoint);
		String startKeyTemp = Bytes.toString(this.region.getStartKey());
//		LOG.info("splitPoint==========" + splitTemp);
//		LOG.info("startKey==========" + startKeyTemp);

		//如果splitPoint和region起始行键一致
		if (splitTemp.contains(startKeyTemp)) {
			// scan扫描找到该region下的中间点，由于split时间假设是在晚上，因此无需使用协处理统计行数。
//			LOG.info("region startKey===========" +Bytes.toString(this.region.getStartKey()));
			byte[] startKey = this.region.getStartKey();
			byte[] endKey = this.region.getEndKey();
			Filter filter = new FirstKeyOnlyFilter();
			Scan scan = new Scan();
			// 从Region的startKey的下一个行键开始搜索，原因：除去索引
			String start = Bytes.toString(startKey);
			if (start == null) {
				try {
					throw new Exception("不能split");
				} catch (Exception e) {
					e.printStackTrace();
					return null;

				}
			}
			//String s = start.substring(0, start.length() - 1) + "1";
			scan.setStartRow(startKey);
			scan.setStopRow(endKey);
			scan.addFamily(Bytes.toBytes("attributes"));
			scan.addFamily(Bytes.toBytes("objects"));
			scan.addFamily(Bytes.toBytes("array_objects"));

			scan.setFilter(filter);
//			scan.setCaching(1000);
			InternalScanner resultScanner = null;
			InternalScanner resultScanner2 = null;
			try {
				resultScanner = this.region.getScanner(scan);
				resultScanner2 = this.region.getScanner(scan);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			int rowCount = 0;
			List<Cell> results = new ArrayList<>();
			try {
				boolean hasMore = false;
				 do{
					hasMore = resultScanner.next(results);
					rowCount++;
					if(rowCount % 2 == 0){
						List<Cell> results2 = new ArrayList<>();
						resultScanner2.next(results2);
						splitPoint = CellUtil.cloneRow(results2.get(0));
//						LOG.info("key:===============" + Bytes.toString(splitPoint));
					}
//					LOG.info("rowCount=========: " + rowCount);
				 }while(hasMore);
				resultScanner.close();
				resultScanner2.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (prefixLength > 0 && splitPoint != null && splitPoint.length > 0) {
			// group split keys by a prefix
			byte[] result = Arrays.copyOf(splitPoint, Math.min(prefixLength, splitPoint.length));
			LOG.info("split key:===============" + Bytes.toString(result));
			//split时插入daughter的块号
			Put put = new Put(result);
	    	put.add(Bytes.toBytes("index"), Bytes.toBytes("1"),result);
			try {
				this.region.put(put);
				this.region.flushcache();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return result;
		} else {
			new Exception("Error split");
			return null;
		}
	}
}
