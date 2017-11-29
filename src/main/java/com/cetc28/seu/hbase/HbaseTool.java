
package com.cetc28.seu.hbase;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter.HexStringSplit;

import com.cetc28.seu.spark.query.model.coprocessor.BuildIndexRegionObserver;
import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyEndPoint;
import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyFinal;
import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyFinal.Answer;
import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyFinal.SearchRequest;
import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyFinal.SearchResponse;
import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyFinal.SearchService;
import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyWithoutIndexEndPoint;
import com.cetc28.seu.spark.query.model.coprocessor.SplitMasterObserver;
import com.google.protobuf.ByteString;
//import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKey.SearchNoIndexService;
//import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKey.SearchRequest;
//import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKey.SearchResponse;
//import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKey.SearchService;
//import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyEndPoint;
//import com.cetc28.seu.spark.query.model.coprocessor.SearchRowKeyWithoutIndexEndPoint;
import com.google.protobuf.ServiceException;


/**
 * Description: a class in order to obtain hbabse configuration, htable and hbaseAdmin.
 * @author ttf
 */
public class HbaseTool {

	private static HbaseTool hbaseTool;
	private static HBaseAdmin hBaseAdmin;
	private static Configuration baseConfiguration;
	private HTableInterface htable;
	private HTableInterface indexTable = null;
	private static HConnection connection;
	private static List<HRegionInfo> hList;
	private HashSet<String> indexHashSet = new HashSet<String>();


	static {
		baseConfiguration = HBaseConfiguration.create();
		// in order to connect remote hbase, zookeeper quorum and client need to
		// be set
		//baseConfiguration.set("hbase.zookeeper.quorum", "ubuntu1");
//		baseConfiguration.set("hbase.zookeeper.quorum","ubuntu1,ubuntu2,ubuntu3");
//		baseConfiguration.set("hbase.zookeeper.quorum","223.3.73.215,223.3.77.59,223.3.92.215");
//		baseConfiguration.setInt("hbase.zookeeper.property.clientPort", 2181);
		baseConfiguration.set("hbase.zookeeper.quorum",HbaseConfig.hostip);
		baseConfiguration.setInt("hbase.zookeeper.property.clientPort", HbaseConfig.port);

		// baseConfiguration.setInt("hbase.zookeeper.property.clientPort",
		// value);
		try {
			connection = HConnectionManager.createConnection(baseConfiguration);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		try {
			hBaseAdmin = new HBaseAdmin(baseConfiguration);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private HbaseTool() {
	}

	public static HBaseAdmin getAdmin() {

		return hBaseAdmin;
	}

	public static Configuration getBaseConfiguration() {
		return baseConfiguration;
	}

	public static void setBaseConfiguration(Configuration baseConfiguration) {
		HbaseTool.baseConfiguration = baseConfiguration;
	}

	public static HbaseTool getInstance() {

		if (hbaseTool == null)
			hbaseTool = new HbaseTool();

		return hbaseTool;
	}
	
	public static List<HRegionInfo> getHList(){
		if(hList == null || hList.size() == 0){
			try {
				hList = hBaseAdmin.getTableRegions(TableName.valueOf(HbaseConfig.tableName));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return hList;
	}

//	public static HashSet<String> getHashSet(){
//		return indexHashSet;
//	}

	public HTableInterface getTable(String tableName) throws IOException {

		if (htable == null) {
/*			try {
				htable = new HTable(baseConfiguration, TableName.valueOf(tableName));
			} catch (IOException e) {
				e.printStackTrace();
			}*/
			htable = connection.getTable(TableName.valueOf(tableName));
			htable.setWriteBufferSize(10*1024*1024);
			htable.setAutoFlushTo(false);
		}

		return htable;

	}

	/**
	 * Description : create a table in the hbase.
	 * 
	 * @param themeName
	 *            the name of table will be created in the hbase.
	 * @param families
	 * @throws IOException
	 */
//	public static void createTable(String themeName,String[] families) throws IOException {
//		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(themeName.getBytes()));
//		HColumnDescriptor[] colDesArray=new HColumnDescriptor[families.length];
//		for(int i=0;i<families.length;i++){
//			colDesArray[i]=new HColumnDescriptor(families[i]);
//			
//			//colDesArray[i].setMaxVersions(1000);
//			hTableDescriptor.addFamily(colDesArray[i]);
//		}
//		hBaseAdmin.createTable(hTableDescriptor);
//	}
	
	
	
	/**
	 * 在建表时加载协处理器,增加了index这一个family
	 * @param tableName
	 */
	public static void createTable(String tableName,String[] families)
	{
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		HColumnDescriptor[] colDesArray=new HColumnDescriptor[families.length];
		for(int i=0;i<families.length;i++){
			colDesArray[i]=new HColumnDescriptor(families[i]);
			colDesArray[i].setBloomFilterType(BloomType.ROW);
			colDesArray[i].setMaxVersions(1);
			hTableDescriptor.addFamily(colDesArray[i]);
		}

		//添加协处理器路径,是namenode的路径,普通jar包就可以
		Path path = new Path("hdfs://223.3.84.42:9000/hbaseCoprocessor/coprocessor.jar");
		try {
			//hTableDescriptor.addCoprocessor(CreateIndexByCop.class.getCanonicalName(), path, Coprocessor.PRIORITY_USER, null);
			hTableDescriptor.addCoprocessor(SearchRowKeyEndPoint.class.getCanonicalName(), path, Coprocessor.PRIORITY_USER, null);	
			hTableDescriptor.addCoprocessor(SplitMasterObserver.class.getCanonicalName(), path, Coprocessor.PRIORITY_USER, null);		
//			hTableDescriptor.addCoprocessor(BuildIndexRegionObserver.class.getCanonicalName(),path,Coprocessor.PRIORITY_USER,null);

		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		//设10个region块，因为索引表与数据放在一起
		byte[][] regions = new byte[][]{
			Bytes.toBytes("0000"),
			Bytes.toBytes("1000"),
			Bytes.toBytes("2000"),
			Bytes.toBytes("3000"),
			Bytes.toBytes("4000"),
			Bytes.toBytes("5000"),
			Bytes.toBytes("6000"),
			Bytes.toBytes("7000"),
			Bytes.toBytes("8000"),
			Bytes.toBytes("9000"),
			Bytes.toBytes("9999a"),
			//增加region，用来保存键的索引列，region为9999a
			Bytes.toBytes("aaaaa"),
		};
		
		try {
			hBaseAdmin.createTable(hTableDescriptor,regions);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("builtTable");

	}
	
	/**
	 * 根据自身所输入的region块个数进行自动划分，若不输入region块的个数则默认为10个,最好输入region块为10的倍数。若不输入为倍数，则会自动变为10的倍数
	 * @param tableName
	 * @param families
	 * @param regionNumber
	 */
	public static void createTable(String tableName,String[] families, int regionNumber)
	{
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		HColumnDescriptor[] colDesArray=new HColumnDescriptor[families.length];
		for(int i=0;i<families.length;i++){
			colDesArray[i]=new HColumnDescriptor(families[i]);
			colDesArray[i].setBloomFilterType(BloomType.ROW);
			colDesArray[i].setMaxVersions(1);
			hTableDescriptor.addFamily(colDesArray[i]);
		}

		//添加协处理器路径,是namenode的路径,普通jar包就可以
		Path path = new Path("hdfs://223.3.84.42:9000/hbaseCoprocessor/coprocessor.jar");
		try {
			hTableDescriptor.addCoprocessor(SearchRowKeyEndPoint.class.getCanonicalName(), path, Coprocessor.PRIORITY_USER, null);	
//			hTableDescriptor.addCoprocessor(SearchRowKeyWithoutIndexEndPoint.class.getCanonicalName(), path, Coprocessor.PRIORITY_USER, null);		
			hTableDescriptor.addCoprocessor(SplitMasterObserver.class.getCanonicalName(), path, Coprocessor.PRIORITY_USER, null);		
//			hTableDescriptor.addCoprocessor(BuildIndexRegionObserver.class.getCanonicalName(),path,Coprocessor.PRIORITY_USER,null);
			//ConstantSizeRegionSplitPolicy.class.表示按照固定大小进行切分。设置表为禁止自动切分DisabledRegionSplitPolicy.class.
//			hTableDescriptor.setRegionSplitPolicyClassName(ConstantSizeRegionSplitPolicy.class.getName());
			hTableDescriptor.setValue(HTableDescriptor.SPLIT_POLICY, CustomKeyPrefixRegionSplitPolicy.class.getName());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			byte[][] regions = getRegions(regionNumber);
			hBaseAdmin.createTable(hTableDescriptor,regions);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("builtTable");

	}
	
	/**
	 * 输入要分裂的Region名称，Region名称可以通过Web UI查看
	 */
	public static void split(String regionName){
//		HashSet<String> hashSet = new HashSet<String>();
		if(hList == null || hList.size() == 0){
			try {
				hList = hBaseAdmin.getTableRegions(TableName.valueOf(HbaseConfig.tableName));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		byte[] startKey = null;
		byte[] endKey = null;
		for(HRegionInfo h : hList){
//			hashSet.add(h.getRegionNameAsString());
			if(h.getRegionNameAsString().equals(regionName)){
				startKey = h.getStartKey();
				endKey = h.getEndKey();
			}
		}
		//scan扫描找到该region下的中间点，由于split时间假设是在晚上，因此无需使用协处理统计行数。
		Filter filter = new FirstKeyOnlyFilter();
		Scan scan = new Scan();
		//从Region的startKey的下一个行键开始搜索，原因：除去索引
		String start = Bytes.toString(startKey);
		if(start == null){
			try {
				throw new Exception("不能split");
			} catch (Exception e) {
				e.printStackTrace();
				return;

			}
		}
		String s = start.substring(0, start.length()-1)+"1";
		System.out.println("s: " + s);
		scan.setStartRow(Bytes.toBytes(s));
		scan.setStopRow(endKey);
		scan.setFilter(filter);
		scan.setCaching(1000);
		ResultScanner resultScanner = null;
		try {
			resultScanner = HbaseTool.getInstance().getTable(HbaseConfig.tableName).getScanner(scan);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		int rowCount = 0;
		for(Result result : resultScanner){
			rowCount++;
		}
		resultScanner.close();
		System.out.println("rowCount: " +rowCount);
		int halfCount = rowCount / 2;
		System.out.println("halfCount: " +halfCount);
		rowCount = 0;
		String rowKey = "";
		
		try {
			resultScanner = HbaseTool.getInstance().getTable(HbaseConfig.tableName).getScanner(scan);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		for(Result result : resultScanner){
			rowCount++;
			System.out.println("rowKey: " + Bytes.toString(result.getRow()));
			if(halfCount == rowCount){
				rowKey = Bytes.toString(result.getRow());
				break;
			}
		}
		resultScanner.close();
		
		System.out.println("choose rowKey: " + rowKey);
		String splitKey = rowKey.substring(0, 4);
		System.out.println("splitKey: " + splitKey);
		try {
			hBaseAdmin.split(regionName, splitKey);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("split has completed");
		
		HbaseTool.getInstance().rebuildIndexAfterSplit();
	/*	//建立索引
		try {
			while(hList.size() == hBaseAdmin.getTableRegions(TableName.valueOf(HbaseConfig.tableName)).size()){
			}
			hList = hBaseAdmin.getTableRegions(TableName.valueOf(HbaseConfig.tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}
		for(HRegionInfo h : hList){
			//System.out.println(h.getRegionNameAsString() + " " + h.isSplit());
			boolean b = false;
			if(!hashSet.contains(h.getRegionNameAsString())){
				String[] str = h.getRegionNameAsString().split(",");
				System.out.println(str[1]);
				for(String hstr : hashSet){
					//原region块删除原有索引，新建索引
					if(hstr.contains(str[1])){
						System.out.println("old region: " + h.getRegionNameAsString());
						HbaseTool.getInstance().deleteIndex(h);
						HbaseTool.getInstance().buildIndex(h,null);
						b = true;
					}

				}
				if(b == false){
					//新region块建索引
					System.out.println("new region: " + h.getRegionNameAsString());
					HbaseTool.getInstance().buildIndex(h,null);
				}

			}
		}
*/
	}
	
	/**
	 * split完后重建索引
	 */
	public void rebuildIndexAfterSplit(){
		List<HRegionInfo> hList = HbaseTool.getHList();
		HashSet<String> hashSet = new HashSet<String>();
		for(HRegionInfo h : hList){
			hashSet.add(h.getRegionNameAsString());
		}
		//后台自动重建索引
		try {
			while(hList.size() == HbaseTool.getAdmin().getTableRegions(TableName.valueOf(HbaseConfig.tableName)).size()){
			}
			hList = HbaseTool.getAdmin().getTableRegions(TableName.valueOf(HbaseConfig.tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}
		for(HRegionInfo h : hList){
			//System.out.println(h.getRegionNameAsString() + " " + h.isSplit());
			boolean b = false;
			if(!hashSet.contains(h.getRegionNameAsString())){
				String[] str = h.getRegionNameAsString().split(",");
				System.out.println(str[1]);
				for(String hstr : hashSet){
					//原region块删除原有索引，新建索引
					if(hstr.contains(str[1])){
						System.out.println("old region: " + h.getRegionNameAsString());
						HbaseTool.getInstance().deleteIndex(h);
						HbaseTool.getInstance().buildIndex(h,null);
						b = true;
					}

				}
				if(b == false){
					//新region块建索引
					System.out.println("new region: " + h.getRegionNameAsString());
					HbaseTool.getInstance().buildIndex(h,null);
				}

			}
		}
	}
	/**
	 * split后删除原region的无效索引
	 */
	public void deleteIndex(HRegionInfo h){
		Scan scan = new Scan();
		Filter filter = new FirstKeyOnlyFilter();
		String start = Bytes.toString(h.getStartKey())+":";
		System.out.println("start: " + start);
		scan.setStartRow(Bytes.toBytes(start));
		String end = Bytes.toString(h.getStartKey())+";";
		System.out.println("end: " + end);
		scan.setStopRow(Bytes.toBytes(end));
		scan.addFamily(Bytes.toBytes("index"));
		scan.setFilter(filter);
		try {
			ResultScanner rs = htable.getScanner(scan);
			List<Delete> deletes = new ArrayList<Delete>();
			for(Result r : rs){
				Delete delete = new Delete(r.getRow());
				deletes.add(delete);
			}
			rs.close();
			htable.delete(deletes);
			htable.flushCommits();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	
	/**
	 * 根据输入的Region块数，设置每个Region块的起始行键
	 * @param num
	 * @return
	 * @throws Exception
	 */
	private static byte[][] getRegions(int num) throws Exception{
		if(num > 10000){
			throw new Exception("输入块数请<10000");
		}
		if((num >= 10) && (num % 10 != 0)){
			int t = num / 10; 
			num = t * 10;
		}
		int sum = 10000/num;
		byte[][] region = new byte[num+2][];
		region[0] = Bytes.toBytes("0000");
		int regionNumber = 0;
		for(int i = 1; i < num; i++){
			String s = "";
			//补0
			regionNumber = regionNumber + sum;
			if(regionNumber < 1000){
				int num0 = 0;
				int temp = regionNumber;
				while(temp != 0){
					temp = temp / 10;
					num0++;
				}
				int com0 = 4 - num0;
				for(int j = 0; j < com0; j++){
					s = s+"0";
				}
				s = s + regionNumber;
			}else{
				//不补0
				s = String.valueOf(regionNumber);
			}
			//System.out.println("prefix: " + s);
			region[i] = Bytes.toBytes(s);
		}
		region[num] = Bytes.toBytes("9999a");
		region[num+1] = Bytes.toBytes("aaaaa");
		return region;
	}
	
/*	//带有版本号的建表
	public static void createTable(String themeName,String[] families) throws IOException {
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(themeName.getBytes()));
		HColumnDescriptor[] colDesArray=new HColumnDescriptor[families.length];
		for(int i=0;i<families.length;i++){
			colDesArray[i]=new HColumnDescriptor(families[i]);
			if(i != 0)
			{
				colDesArray[i].setMaxVersions(5000);
			}
			hTableDescriptor.addFamily(colDesArray[i]);
		}
		hBaseAdmin.createTable(hTableDescriptor);
	}*/
	
	public HTableInterface getIndexTable(String indexTableName) throws IOException{
		if(indexTable == null)
		{
			//connection = HConnectionManager.createConnection(baseConfiguration);
			indexTable = connection.getTable(TableName.valueOf(indexTableName));
			indexTable.setWriteBufferSize(10*1024*1024);
			indexTable.setAutoFlushTo(false);
		}
		return indexTable;

	}
	
	public void createIndexTable(String indexTableName)
	{
		HTableDescriptor hTableIndex = new HTableDescriptor(TableName.valueOf(indexTableName));
		HColumnDescriptor hColumnDescriptor_Row = new HColumnDescriptor("Row");

		hTableIndex.addFamily(hColumnDescriptor_Row);
		try {
			hBaseAdmin.createTable(hTableIndex);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	
	/**
	 * split完成后对region重新做索引
	 * @param region
	 */
	public void buildIndex(HRegionInfo region,HashSet<String> indexHashSet){
		String start = Bytes.toString(region.getStartKey());
		System.out.println("start key: " + start);
		String end = Bytes.toString(region.getEndKey());
		//System.out.println(start+"   "+end);
		Scan scan = new Scan();
		scan.setStartRow(region.getStartKey());
		scan.setStopRow(region.getEndKey());
		ResultScanner resultScanner = null;
		try {
			resultScanner = htable.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}
		for(Result result : resultScanner)
		{
			String dataIndex = Bytes.toString(result.getRow());
			String regionNumber = start;
			ArrayList<Put> arrayList = new ArrayList<Put>();
			for(Cell cell : result.rawCells())
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
					//对建立索引的列存放列名
					if(indexHashSet != null)
						indexHashSet.add(column);
					
					Put put = new Put(Bytes.toBytes(regionNumber+":"+value+":"+family+":"+column+"_"+dataIndex));
					put.add(Bytes.toBytes("index"), Bytes.toBytes("1"), Bytes.toBytes(dataIndex));
					arrayList.add(put);
				}else if(value.matches(s1) || value.matches(s2) || value.equals("null") || value.equals("Null") || (family.equals("attributes") && column.equals("data")) || family.equals("index")){
					continue;
				}else{	
					//对建立索引的列存放列名
					if(indexHashSet != null)
						indexHashSet.add(column);
					
					Put put = new Put(Bytes.toBytes(regionNumber+":"+value+":"+family+":"+column+"_"+dataIndex));
					put.add(Bytes.toBytes("index"), Bytes.toBytes("1"), Bytes.toBytes(dataIndex));
					arrayList.add(put);
				}
				try {
					htable.put(arrayList);
					htable.flushCommits();
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
		}
		resultScanner.close();
	}

	/**
	 * 表建完后生成索引
	 */
	public void buildIndex()
	{
		HashSet<String> indexHashSet = new HashSet<String>();
		
		Scan scan = new Scan();
		ResultScanner resultScanner = null;
		try {
			resultScanner = htable.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}
		for(Result result : resultScanner)
		{
			String dataIndex = Bytes.toString(result.getRow());
			String dataIndexTemp = dataIndex.substring(0, 1);
			String regionNumber = getRegionNumber(dataIndexTemp);
			ArrayList<Put> arrayList = new ArrayList<Put>();
			for(Cell cell : result.rawCells())
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
					//对建立索引的列存放列名
					indexHashSet.add(column);
					
					Put put = new Put(Bytes.toBytes(regionNumber+":"+value+":"+family+":"+column+"_"+dataIndex));
					put.add(Bytes.toBytes("index"), Bytes.toBytes("1"), Bytes.toBytes(dataIndex));
					arrayList.add(put);
				}else if(value.matches(s1) || value.matches(s2) || value.equals("null") || value.equals("Null") || (family.equals("attributes") && column.equals("data"))){
					continue;
				}else{
					//对建立索引的列存放列名
					indexHashSet.add(column);
					
					Put put = new Put(Bytes.toBytes(regionNumber+":"+value+":"+family+":"+column+"_"+dataIndex));
					put.add(Bytes.toBytes("index"), Bytes.toBytes("1"), Bytes.toBytes(dataIndex));
					arrayList.add(put);
				}
				try {
					htable.put(arrayList);
					htable.flushCommits();

				} catch (IOException e) {
				// TODO Auto-generated catch block
					e.printStackTrace();
				}	
			}
		}
		resultScanner.close();
		//将建立索引的列存储起来，放入文件
		outputFile(indexHashSet);
	}
	
	/**
	 * 将建立文件的列存起来，放入文件，即使split后，由于表数据未变，所以不需要动态加入
	 * @param indexHashSet
	 */
	private void outputFile(HashSet<String> indexHashSet){
		
		try{
			File file=new File("ColumnName.txt");
	        if(file.exists()){
	            file.delete();
	        }
	        if(!file.exists()){
	        	file.createNewFile();
	        }
	        FileWriter fw = new FileWriter(file, true);  
            BufferedWriter bw = new BufferedWriter(fw);
            StringBuffer sb=new StringBuffer();
            int i = 0;
            Put put = new Put(Bytes.toBytes("9999a"));
	        for(String str : indexHashSet){
	            sb.append(str +"\r\n");
	            //放入表中的region为9999a的部分
	            put.add(Bytes.toBytes("attributes"), Bytes.toBytes(String.valueOf(i)), Bytes.toBytes(str));
	            i++;
	        }        
            bw.write(sb.toString());
            bw.flush();
	        bw.close();	
	        htable.put(put);
	        htable.flushCommits();
	        
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	/**
	 * 将column的名字从file中读取，若无file，从hbase表中读取数据然后放到file中
	 * @return hashSet
	 */
	public HashSet<String> inputFile(){
        HashSet<String> indexHashSet = new HashSet<String>();
		  try {  
	            File file = new File("ColumnName.txt");  
	            //当文件不存在时，从数据库读取之前建立的关键字表
	            if(!file.exists()){
	            	HashSet<String> hashSet = new HashSet<String>();
	            	Get get = new Get(Bytes.toBytes("9999a"));
	            	Result result = htable.get(get);
	            	for(Cell cell : result.rawCells()){
	            		hashSet.add(Bytes.toString(CellUtil.cloneValue(cell)));
	            	}
	            	outputFile(hashSet);
	            }
	            // 读取文件，并且以utf-8的形式写出去
	            BufferedReader bufread;  
	            String read;  
	            bufread = new BufferedReader(new FileReader(file));
	            StringBuffer sb = new StringBuffer();
	            while ((read = bufread.readLine()) != null) {  
	                sb.append(read+" ");  
	            }
	            String[] str = sb.toString().split(" ");
	            for(String s : str){
	            	indexHashSet.add(s);
	            }
	            bufread.close();  

	        } catch (FileNotFoundException ex) {  
	            ex.printStackTrace();  
	        } catch (IOException ex) {  
	            ex.printStackTrace();  
	        }  
        return indexHashSet;
	}
	
	
	/**
	 * 启动协处理器进行检索,对于有索引的部分进行查找，查阅资料得到是快方法，但是实际使用速度不快，有callback方法
	 * @param tableName
	 * @param family
	 * @param column
	 * @param value
	 * @return
	 */
	public List<ByteString> startSearchIndex(String family, List<String> column, List<String> value)
	{
		List<ByteString>  rowKey = new ArrayList<ByteString>();
		final SearchRequest.Builder build = SearchRequest.newBuilder();
		build.setFamily(family);
		build.addAllColumn(column);
		build.addAllValue(value);
		SearchRequest request = build.build();	   
		try{
			Batch.Call<SearchService, List<ByteString>> callable = new Batch.Call<SearchService, List<ByteString>>(){
	
				@Override
				public List<ByteString> call(SearchService instance) throws IOException {
					BlockingRpcCallback<SearchResponse> rpcCallback = new BlockingRpcCallback<SearchResponse>();
					instance.getRowKey(null, request, rpcCallback);
		            SearchResponse response = rpcCallback.get();
		            System.out.println("response.getRowKeyCount()" + response.getAnswerCount());
		            if(response.getAnswerCount()==0)
		            {
		            	return new ArrayList<ByteString>();
		            }
		            else
		            {
		            	return response.getAnswerList();
		            }
				}
				
			}; 
			
			Batch.Callback<List<ByteString>> callback = new Batch.Callback<List<ByteString>>(){
	
				@Override
				public void update(byte[] region, byte[] row, List<ByteString> result) {
					rowKey.addAll(result);
				}
				
			};
				htable.coprocessorService(SearchService.class, null, null, callable, callback);
		}catch (Throwable e) {
				e.printStackTrace();
		}
		System.out.println("num2:" + rowKey.size());
	    return rowKey;

	}
	
	/**
	 * 启动协处理器进行检索,对于有索引的部分进行查找,查阅资料得到是慢方法，但是实际使用速度不慢，没有callback方法
	 * @param tableName
	 * @param family
	 * @param column
	 * @param value
	 * @return
	 */
//	public List<ByteString> startSearchIndex(String family, List<String> column, List<String> value)
//	{
//		List<ByteString>  rowKey = new ArrayList<ByteString>();
//		final SearchRequest.Builder build = SearchRequest.newBuilder();
//		build.setFamily(family);
//		build.addAllColumn(column);
//		build.addAllValue(value);
//		SearchRequest request = build.build();	    	
//	    	try {
//	    		Map<byte[], List<ByteString>> result = htable.coprocessorService(SearchService.class, null, null, new Batch.Call<SearchService, List<ByteString>>() {
//	
//					@Override
//					public List<ByteString> call(SearchService instance) throws IOException {
//						BlockingRpcCallback<SearchResponse> rpcCallback = new BlockingRpcCallback<SearchResponse>();
//						instance.getRowKey(null, request, rpcCallback);
//			            SearchResponse response = rpcCallback.get();
//			            System.out.println("response.getRowKeyCount()" + response.getAnswerCount());
//			            if(response.getAnswerCount()==0)
//			            {
//			            	return new ArrayList<ByteString>();
//			            }
//			            else
//			            {
//			            	return response.getAnswerList();
//			            }
//			            
//					}
//				});
//				System.out.println("str:" + result.size());
//				for(List<ByteString> resultValue : result.values())
//	    		{
//	    				rowKey.addAll(resultValue);
//
//	    		}
//				System.out.println("num2:" + rowKey.size());
//
//			} catch (ServiceException e) {
//				e.printStackTrace();
//			} catch (Throwable e) {
//				e.printStackTrace();
//			}
//	    	return rowKey;
//
//	}
	
	
	
//	/**
//	 * 启动协处理器进行检索,对于有索引的部分进行查找
//	 * @param tableName
//	 * @param family
//	 * @param column
//	 * @param value
//	 * @return
//	 */
//	public List<String> startSearchIndex(String family, String column, String value)
//	{
//		List<String>  rowKey = new ArrayList<String>();
//		final SearchRequest request = SearchRequest.newBuilder().setFamily(family).setColumn(column).setValue(value).build();
//	    	
//	    	try {
//	    		Map<byte[], List<String>> result = htable.coprocessorService(SearchService.class, null, null, new Batch.Call<SearchService, List<String>>() {
//	
//					@Override
//					public List<String> call(SearchService instance) throws IOException {
//						// TODO Auto-generated method stub
//						BlockingRpcCallback<SearchResponse> rpcCallback = new BlockingRpcCallback<SearchResponse>();
//						instance.getRowKey(null, request, rpcCallback);
//			            SearchResponse response = rpcCallback.get();
//			            System.out.println("response.getRowKeyCount()" + response.getRowKeyCount());
//			            if(response.getRowKeyCount()==0)
//			            {
//			            	return new ArrayList<String>();
//			            }
//			            else
//			            {
//			            	return response.getRowKeyList();
//			            }
//			            
//					}
//				});
//				System.out.println("str:" + result.size());
//				for(List<String> resultValue : result.values())
//	    		{
//	    			for(String str : resultValue){
//	    				rowKey.add(str);
//	    			}
//
//	    		}
//				System.out.println("num2:" + rowKey.size());
//
//			} catch (ServiceException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} catch (Throwable e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//	    	
//	    	return rowKey;
//	}
	
	
	
//	/**
//	 * 启动协处理器进行检索,对于没有索引的部分进行查找
//	 * @param tableName
//	 * @param family
//	 * @param column
//	 * @param value
//	 * @return
//	 */
//	public List<String> startSearch(String family, String column, String value)
//	{
//		List<String>  rowKey = new ArrayList<String>();
//		final SearchRequest request = SearchRequest.newBuilder().setFamily(family).setColumn(column).setValue(value).build();
//	    	
//	    	try {
//	    		Map<byte[], List<String>> result = htable.coprocessorService(SearchNoIndexService.class, null, null, new Batch.Call<SearchNoIndexService, List<String>>() {
//	
//					@Override
//					public List<String> call(SearchNoIndexService instance) throws IOException {
//						// TODO Auto-generated method stub
//						BlockingRpcCallback<SearchResponse> rpcCallback = new BlockingRpcCallback<SearchResponse>();
//						instance.getRowKeyNoIndex(null, request, rpcCallback);
//			            SearchResponse response = rpcCallback.get();
//			            System.out.println("SearchNoIndexService response.getRowKeyCount()" + response.getRowKeyCount());
//			            if(response.getRowKeyCount()==0)
//			            {
//			            	return new ArrayList<String>();
//			            }
//			            else
//			            {
//			            	return response.getRowKeyList();
//			            }
//			            
//					}
//				});
//				System.out.println("str:" + result.size());
//				System.out.println("num:" + result.values().size());
//	    		for(List<String> resultValue : result.values())
//	    		{
//	    			for(String str : resultValue){
//	    				rowKey.add(str);
//	    			}
//
//	    		}
//			} catch (ServiceException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} catch (Throwable e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//	    	
//	    	return rowKey;
//	}
	
	
	
	
	public String getRegionNumber(String id)
	{
		String regionNumber = "";
		switch(id)
		{
			case "0": 
					regionNumber = "0000";
					break;
			case "1": 
					regionNumber = "1000";
					break;
			case "2": 
					regionNumber = "2000";
					break;
			case "3": 
					regionNumber = "3000";
					break;
			case "4": 
					regionNumber = "4000";
					break;
			case "5": 
					regionNumber = "5000";
					break;
			case "6": 
					regionNumber = "6000";
					break;
			case "7": 
					regionNumber = "7000";
					break;
			case "8": 
					regionNumber = "8000";
					break;
			case "9": 
					regionNumber = "9000";
					break;
		}
		return regionNumber;
	}
	
	/**
	 * 随机获取前缀
	 * @return
	 */
	public String getRandomPrefix()
	{
		String base = "0123456789";   
	    Random random = new Random();   
	    StringBuffer sb = new StringBuffer();   
	    for (int randomNumber = 0; randomNumber < 4; randomNumber++) {   
	        int number = random.nextInt(base.length());   
	        sb.append(base.charAt(number));   
	    }   
	    String prefix = sb.toString();
	    return prefix;
	}
	
	/**
	 * 插入数据时，为了以后的分隔，在有少量的数据冗余性的前提下，插入随机数，在index = 1中，值为随机数值,会牺牲部分写性能
	 * @throws IOException 
	 */
	public void putTableSplit() throws IOException{
		System.out.println("successful read");
		for(int i=1; i<8000; i++)
		{
			String prefix = getRandomPrefix();
//			//加入随机数，在牺牲一部分写性能的情况下
//			Put put1 = new Put(Bytes.toBytes(prefix));
//			put1.add(Bytes.toBytes("index"), Bytes.toBytes("1"), Bytes.toBytes(prefix));
//			htable.put(put1);
			//htable.flushCommits();
			
			String dataIndex = prefix+":test:"+prefix.hashCode()+":"+i;
			Put put = new Put(Bytes.toBytes(dataIndex));
		    int columnNumber = (int)(1+Math.random()*(15-1+1));
		    if(i%22 == 0)
		    {
//		    	System.out.println("dataIndex: " + dataIndex);
		    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("wayne"));
		    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu0"));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("age"),Bytes.toBytes(i+""));
				put.add(Bytes.toBytes("objects"), Bytes.toBytes("parent"),Bytes.toBytes((prefix+":"+1000)+""));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("car"),Bytes.toBytes("Ferrari"));
		    }
		    else if(i%7 == 0)
		    {
		    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("think"));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu" + i));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("age"),Bytes.toBytes(i+""));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("car"),Bytes.toBytes("Benz"));
				put.add(Bytes.toBytes("array_objects"), Bytes.toBytes("random"),Bytes.toBytes(prefix));
		    }
		    else if(i%37 == 0)
		    {
		    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("One"));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu" + i));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("age"),Bytes.toBytes(i+""));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("car"),Bytes.toBytes("Benz"));
				put.add(Bytes.toBytes("array_objects"), Bytes.toBytes("random"),Bytes.toBytes(prefix));
		    }
		    else
		    {
		    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes((prefix+i)+""));
		    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("s-id"),Bytes.toBytes((prefix+i)+""));
		    	put.add(Bytes.toBytes("attributes"),Bytes.toBytes((i+1)+""),Bytes.toBytes((prefix+i)+""));
		    	put.add(Bytes.toBytes("attributes"),Bytes.toBytes((i+1)+""),Bytes.toBytes("Null"));
			    
		    }
		    htable.put(put);
			htable.flushCommits();

		}

//		String str1 = "0111";
//		Put put2 = new Put(Bytes.toBytes(str1));
//		put2.add(Bytes.toBytes("index"), Bytes.toBytes("1"), Bytes.toBytes(str1));
//		htable.put(put2);
//		htable.flushCommits();

		
		Put put = new Put(Bytes.toBytes("0111:test:-123:1"));
		put.add(Bytes.toBytes("attributes"), Bytes.toBytes("data"),Bytes.toBytes("zhu123"));
		put.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu0"));
		
		htable.put(put);
		htable.flushCommits();
		
		System.out.println("successful insert");
	}
	
	/**
	 * split状态下表建完后生成索引
	 */
	public void buildIndexSplit()
	{
		HashSet<String> indexHashSet = new HashSet<String>();
		try {
			hList = hBaseAdmin.getTableRegions(TableName.valueOf(HbaseConfig.tableName));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		for(HRegionInfo h : hList){
			buildIndex(h,indexHashSet);
		}
		//将建立索引的列存储起来，放入文件
		outputFile(indexHashSet);
	}
	
	/**
	 * 测试分裂的状态
	 */
	public void putSplitTable(){
		Put put1 = new Put(Bytes.toBytes(1000+""));
    	put1.add(Bytes.toBytes("index"), Bytes.toBytes("1"),Bytes.toBytes("1000"));
		for(int i = 1; i < 999; i++){
			String rowKey = 1000 + i +"";
			Put put = new Put(Bytes.toBytes(rowKey));
	    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("wayne"+i));
		    try {
				htable.put(put);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			htable.put(put1);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			htable.flushCommits();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * 最后找出所有索引行都放在9999a中，适合增量添加数据
	 * @param put
	 */
	public void buildServerIndex(Put put){
		
		for(List<Cell> list : put.getFamilyCellMap().values()){
			for(Cell cell : list){
				String family = Bytes.toString(CellUtil.cloneFamily(cell));
				String column = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				//是否为纯数字
				String s1 = "^-?\\d+$";
				//是否为时间
				String s2 = "(\\d{1,4}[-|\\/|年|\\.]\\d{1,2}[-|\\/|月|\\.]\\d{1,2}([日|号])?(\\s)*(\\d{1,2}([点|时])?((:)?\\d{1,2}(分)?((:)?\\d{1,2}(秒)?)?)?)?(\\s)*(PM|AM)?)";
				//字段为bdnm,或包含id
				if((family.equals("attributes") && column.equals("bdnm")) || (family.equals("attributes") && column.contains("id"))){
					//对建立索引的列存放列名
					indexHashSet.add(column);
				}else if(value.matches(s1) || value.matches(s2) || value.equals("null") || value.equals("Null") || (family.equals("attributes") && column.equals("data"))){
					continue;
				}else{
					//对建立索引的列存放列名
					indexHashSet.add(column);
				}
			}
		}
	}
	
	
	/**
	 * 测试用
	 * @throws IOException
	 */
	public void putTable() throws IOException
	{
		
		System.out.println("successful read");
		for(int i=1; i<100000; i++)
		{
			String prefix = getRandomPrefix();
			String indexPrefixTemp = prefix.substring(0, 1);
			String indexPrefix = getRegionNumber(indexPrefixTemp);
			String dataIndex = prefix+":test:"+prefix.hashCode()+":"+i;
			Put put = new Put(Bytes.toBytes(dataIndex));
		    int columnNumber = (int)(1+Math.random()*(15-1+1));
		    if(i%22 == 0)
		    {
//		    	System.out.println("dataIndex: " + dataIndex);
		    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("wayne"));
		    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu0"));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("age"),Bytes.toBytes(i+""));
				put.add(Bytes.toBytes("objects"), Bytes.toBytes("parent"),Bytes.toBytes((prefix+":"+1000)+""));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("car"),Bytes.toBytes("Ferrari"));
		    }
		    else if(i%7 == 0)
		    {
		    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("think"));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu" + i));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("age"),Bytes.toBytes(i+""));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("car"),Bytes.toBytes("Benz"));
				put.add(Bytes.toBytes("array_objects"), Bytes.toBytes("random"),Bytes.toBytes(prefix));
		    }
		    else if(i%37 == 0)
		    {
		    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("One"));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu" + i));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("age"),Bytes.toBytes(i+""));
				put.add(Bytes.toBytes("attributes"), Bytes.toBytes("car"),Bytes.toBytes("Benz"));
				put.add(Bytes.toBytes("array_objects"), Bytes.toBytes("random"),Bytes.toBytes(prefix));
		    }
		    else
		    {
		    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes((prefix+i)+""));
		    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("s-id"),Bytes.toBytes((prefix+i)+""));
		    	put.add(Bytes.toBytes("attributes"),Bytes.toBytes((i+1)+""),Bytes.toBytes((prefix+i)+""));
		    	put.add(Bytes.toBytes("attributes"),Bytes.toBytes((i+1)+""),Bytes.toBytes("Null"));
			    
		    }
		    //有put的行必须有buildServerIndex方法
		    buildServerIndex(put);
		    htable.put(put);
			htable.flushCommits();

		}

		Put put = new Put(Bytes.toBytes("0111:test:-123:1"));
		put.add(Bytes.toBytes("attributes"), Bytes.toBytes("data"),Bytes.toBytes("zhu123"));
		put.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu0"));
	    //有put的行必须有buildServerIndex方法
	    buildServerIndex(put);
		htable.put(put);
		htable.flushCommits();
		
		System.out.println("successful insert");
		
	}
	
	public void putLast(){
		 int i = 0;
         Put put = new Put(Bytes.toBytes("9999a"));
	        for(String str : indexHashSet){
	            //放入表中的region为9999a的部分
	            put.add(Bytes.toBytes("attributes"), Bytes.toBytes(String.valueOf(i)), Bytes.toBytes(str));
	            i++;
	        }        
		try {
			htable.put(put);
			htable.flushCommits();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 测试用，用于测试单个join,添加parent数据
	 */
	public void addOneRecord(){
		System.out.println("start");
		for(int i=1; i<10; i++)
		{
			String prefix = getRandomPrefix();
			String indexPrefixTemp = prefix.substring(0, 1);
			String indexPrefix = getRegionNumber(indexPrefixTemp);
			String dataIndex = prefix+":test123:"+prefix.hashCode()+":"+i;
			
			Put put = new Put(Bytes.toBytes(dataIndex));	    	
	    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("Rooney"));
	    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu0"));
			put.add(Bytes.toBytes("attributes"), Bytes.toBytes("age"),Bytes.toBytes(i+""));
			String t = prefix.substring(0, 3)+"0:topic:"+prefix.hashCode()+":"+i;
			put.add(Bytes.toBytes("objects"), Bytes.toBytes("parent"),Bytes.toBytes(t));
			put.add(Bytes.toBytes("attributes"), Bytes.toBytes("car"),Bytes.toBytes("Ferrari"));
			try {
			    //有put的行必须有buildServerIndex方法
			    buildServerIndex(put);
			    htable.put(put);
				htable.flushCommits();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			Put put1 = new Put(Bytes.toBytes(t));
	    	put1.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("Man"));
			put1.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu" + i));
			put1.add(Bytes.toBytes("attributes"), Bytes.toBytes("age"),Bytes.toBytes(i+""));
			put1.add(Bytes.toBytes("attributes"), Bytes.toBytes("car"),Bytes.toBytes("Benz"));
		   
		    try {
			    //有put的行必须有buildServerIndex方法
			    buildServerIndex(put1);
				htable.put(put1);
				htable.flushCommits();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		System.out.println("end");
		
	}
	
	public void putDelete() throws IOException{
		Put put1 = new Put(Bytes.toBytes("0001"));
		put1.add(Bytes.toBytes("index"), Bytes.toBytes("1"),Bytes.toBytes("1"));
		
		Put put = new Put(Bytes.toBytes("0001:test"));	    	
		put.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("Man"));

		Put put2 = new Put(Bytes.toBytes("0001:aest2"));	    	
		put2.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("Man"));
		put2.add(Bytes.toBytes("index"), Bytes.toBytes("1"),Bytes.toBytes("Man"));
		
		htable.put(put1);
		htable.put(put2);
		htable.put(put);
		htable.flushCommits();
		
	}
	
	/**
	 * 建增量索引
	 * @param put
	 */
	public void addIndexIncrement(Put dataPut){
		String dataIndex = Bytes.toString(dataPut.getRow());
		String dataIndexTemp = dataIndex.substring(0, 1);
		String regionNumber = getRegionNumber(dataIndexTemp);
		System.out.println(dataIndex);
		
		for(Entry<byte[], List<Cell>> entry : dataPut.getFamilyCellMap().entrySet()){
			ArrayList<Put> arrayList = new ArrayList<Put>();
			for(Cell cell : entry.getValue()){
				String family = Bytes.toString(CellUtil.cloneFamily(cell));
				String column = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				System.out.println(family +"　"+column+" "+value);
				
				//是否为纯数字
				String s1 = "^-?\\d+$";
				//是否为时间
				String s2 = "(\\d{1,4}[-|\\/|年|\\.]\\d{1,2}[-|\\/|月|\\.]\\d{1,2}([日|号])?(\\s)*(\\d{1,2}([点|时])?((:)?\\d{1,2}(分)?((:)?\\d{1,2}(秒)?)?)?)?(\\s)*(PM|AM)?)";
				//字段为bdnm,或包含id
				if((family.equals("attributes") && column.equals("bdnm")) || (family.equals("attributes") && column.contains("id"))){
					Put put = new Put(Bytes.toBytes(regionNumber+":"+value+":"+family+":"+column+"_"+dataIndex));
					put.add(Bytes.toBytes("index"), Bytes.toBytes("1"), Bytes.toBytes(dataIndex));
					arrayList.add(put);
				}else if(value.matches(s1) || value.matches(s2) || value.equals("Null") || (family.equals("attributes") && column.equals("data"))){
					continue;
				}else{
					Put put = new Put(Bytes.toBytes(regionNumber+":"+value+":"+family+":"+column+"_"+dataIndex));
					put.add(Bytes.toBytes("index"), Bytes.toBytes("1"), Bytes.toBytes(dataIndex));
					arrayList.add(put);
				}
				try {
					htable.put(arrayList);
					htable.flushCommits();

				} catch (IOException e) {
				// TODO Auto-generated catch block
					e.printStackTrace();
				}	
			}
		}
	}
	
	
	/**
	 * 测试两个Join
	 */
	public void addTwoJoin(){
		System.out.println("start");
		for(int i=1; i<10; i++)
		{
			String prefix = getRandomPrefix();
			String indexPrefixTemp = prefix.substring(0, 1);
			String indexPrefix = getRegionNumber(indexPrefixTemp);
			String dataIndex = prefix+":judge:"+prefix.hashCode()+":"+i;
			
			Put put = new Put(Bytes.toBytes(dataIndex));	    	
	    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("Jack"));
	    	put.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu0"));
			put.add(Bytes.toBytes("attributes"), Bytes.toBytes("age"),Bytes.toBytes(i+""));
			String t = prefix.substring(0, 3)+"0:topic:"+prefix.hashCode()+":"+i;
			put.add(Bytes.toBytes("objects"), Bytes.toBytes("parent"),Bytes.toBytes(t));
			put.add(Bytes.toBytes("attributes"), Bytes.toBytes("car"),Bytes.toBytes("Ferrari"));
			try {
			    //有put的行必须有buildServerIndex方法
			    buildServerIndex(put);
			    htable.put(put);
				htable.flushCommits();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			Put put1 = new Put(Bytes.toBytes(t));
	    	put1.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("Rose"));
			put1.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu" + i));
			put1.add(Bytes.toBytes("attributes"), Bytes.toBytes("age"),Bytes.toBytes(i+""));
			put1.add(Bytes.toBytes("attributes"), Bytes.toBytes("car"),Bytes.toBytes("Benz"));
			String s = prefix.substring(0, 3)+"5:result:"+prefix.hashCode()+":"+i;
			put1.add(Bytes.toBytes("objects"), Bytes.toBytes("parent"),Bytes.toBytes(s));
		    try {
			    //有put的行必须有buildServerIndex方法
			    buildServerIndex(put1);
				htable.put(put1);
				htable.flushCommits();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    
			Put put2 = new Put(Bytes.toBytes(s));
	    	put2.add(Bytes.toBytes("attributes"), Bytes.toBytes("bdnm"),Bytes.toBytes("Baby"));
			put2.add(Bytes.toBytes("attributes"), Bytes.toBytes("className"),Bytes.toBytes("zhu" + i));
			put2.add(Bytes.toBytes("attributes"), Bytes.toBytes("age"),Bytes.toBytes(i+""));
			put2.add(Bytes.toBytes("attributes"), Bytes.toBytes("car"),Bytes.toBytes("Benz"));
		    try {
			    //有put的行必须有buildServerIndex方法
			    buildServerIndex(put2);
				htable.put(put2);
				htable.flushCommits();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    

		}

		System.out.println("end");
		
	}

	
	/**
	 * close the connection.
	 */
	public void close() {
		try {
			htable.close();
			connection.close();
			hBaseAdmin.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}


/*public void buildIndexSplit()
{
	HashSet<String> indexHashSet = new HashSet<String>();
	try {
		hList = hBaseAdmin.getTableRegions(TableName.valueOf(HbaseConfig.tableName));
	} catch (IOException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	for(HRegionInfo h : hList){
		String start = Bytes.toString(h.getStartKey());
		String end = Bytes.toString(h.getEndKey());
		//System.out.println(start+"   "+end);
		Scan scan = new Scan();
		scan.setStartRow(h.getStartKey());
		scan.setStopRow(h.getEndKey());
		ResultScanner resultScanner = null;
		try {
			resultScanner = htable.getScanner(scan);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for(Result result : resultScanner)
		{
			String dataIndex = Bytes.toString(result.getRow());
			String regionNumber = start;
			ArrayList<Put> arrayList = new ArrayList<Put>();
			for(Cell cell : result.rawCells())
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
					//对建立索引的列存放列名
					indexHashSet.add(column);
					
					Put put = new Put(Bytes.toBytes(regionNumber+":"+value+":"+family+":"+column+"_"+dataIndex));
					put.add(Bytes.toBytes("index"), Bytes.toBytes("1"), Bytes.toBytes(dataIndex));
					arrayList.add(put);
				}else if(value.matches(s1) || value.matches(s2) || value.equals("null") || value.equals("Null") || (family.equals("attributes") && column.equals("data"))){
					continue;
				}else{
					//对建立索引的列存放列名
					indexHashSet.add(column);
					
					Put put = new Put(Bytes.toBytes(regionNumber+":"+value+":"+family+":"+column+"_"+dataIndex));
					put.add(Bytes.toBytes("index"), Bytes.toBytes("1"), Bytes.toBytes(dataIndex));
					arrayList.add(put);
				}
				try {
					htable.put(arrayList);
					htable.flushCommits();

				} catch (IOException e) {
				// TODO Auto-generated catch block
					e.printStackTrace();
				}	
			}
		}
		resultScanner.close();
	}
	
	//将建立索引的列存储起来，放入文件
	outputFile(indexHashSet);
}*/
