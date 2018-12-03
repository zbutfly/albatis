package net.butfly.albatis.hbase;

import static net.butfly.albacore.paral.Sdream.of;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import net.butfly.albatis.ddl.TableCustomSet;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.ipc.RemoteWithExtrasException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

import com.hzcominfo.albatis.nosql.Connection;
import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;

public class HbaseConnection extends NoSqlConnection<org.apache.hadoop.hbase.client.Connection> {
	private final static Logger logger = Logger.getLogger(HbaseConnection.class);

	static {
		logger.warn(
				"Hbase client common lib not support 2 digitals jdk version (checked by \"\\d\\.\\d\\..*\" in org.apache.hadoop.hbase.util.ClassSize:118) hacking into 9.0.4 -_-");
		System.setProperty("java.version", "9.0.4");
		// XXX: check
		ClassSize.align(ClassSize.OBJECT + 2 * ClassSize.REFERENCE + 1 * Bytes.SIZEOF_LONG + ClassSize.REFERENCE + ClassSize.REFERENCE
				+ ClassSize.TREEMAP);
	}

	private static final int GET_BATCH_SIZE = Integer.parseInt(Configs.gets("albatis.hbase.connection.get.batch.size", "100"));
	private static final int GET_SCAN_OBJS = Integer.parseInt(Configs.gets("albatis.hbase.connection.get.scaner.queue", "500"));
	private static final int GET_MAX_RETRIES = Integer.parseInt(Configs.gets("albatis.hbase.connection.get.retry", "2"));
	private static final int GET_SCAN_BYTES = Integer.parseInt(Configs.gets("albatis.hbase.connection.get.result.bytes", "3145728")); // 3M
	private static final int GET_SCAN_LIMIT = Integer.parseInt(Configs.gets("albatis.hbase.connection.get.result.limit", "1"));
	private static final long GET_STATS_STEP = Long.parseLong(Configs.gets("albatis.hbase.connection.get.stats.step", "-1"));
	private static final long GET_DUMP_MIN_SIZE = Integer.parseInt(Configs.gets("albatis.hbase.connection.get.dump.min.bytes", "2097152")); // 2M
	private final Map<String, Table> tables;
	private final LinkedBlockingQueue<Scan> scans = new LinkedBlockingQueue<>(GET_SCAN_OBJS);
	public final Function<Map<String, Object>, byte[]> conv;

	public HbaseConnection() throws IOException {
		this(new URISpec("hbase:///"));
	}

	public HbaseConnection(URISpec uri) throws IOException {
		super(uri, "hbase");
		conv = Connection.uriser(uri);
		tables = Maps.of();
	}

	@Override
	protected org.apache.hadoop.hbase.client.Connection initialize(URISpec uri) {
		if (null != client) try {
			client.close();
		} catch (Exception e) {
			logger.error("Initialize hbase connection fail on close origin, maybe leak!", e);
		}
		Map<String, String> params = null;
		if (null != uri) {
			params = new ConcurrentHashMap<>(uri.getParameters());
			switch (uri.getScheme()) {
			case "hbase":
				if (uri.getInetAddrs().length == 1) {
					logger.warn("Deprecate master connect to hbase: " + uri.getHost());
					params.put("hbase.master", "*" + uri.getHost() + "*");
					break;
				}
			case "zk":
			case "zookeeper":
			case "hbase:zk":
			case "hbase:zookeeper":
				if (uri.getInetAddrs().length > 0) for (InetSocketAddress a : uri.getInetAddrs()) {
					params.put(HConstants.ZOOKEEPER_QUORUM, a.getHostName());
					params.put(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(a.getPort()));
				}
			}
		}
		try {
			return Hbases.connect(params);
		} catch (IOException e) {
			logger.error("Connect failed", e);
			return null;
		}
	}

	@Override
	public void close() throws IOException {
		super.close();
		for (String k : tables.keySet())
			try {
				Table t = tables.remove(k);
				if (null != t) t.close();
			} catch (IOException e) {
				logger().error("Hbase table [" + k + "] close failure", e);
			}
		Hbases.disconnect(client);
	}

	public Table table(String table) {
		return tables.computeIfAbsent(table, t -> {
			try {
				return client.getTable(TableName.valueOf(t), Hbases.ex);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
	}

	public void put(String table, Mutation op) throws IOException {
		Table t = table(table);
		if (op instanceof Put) t.put((Put) op);
		else if (op instanceof Delete) t.delete((Delete) op);
		else if (op instanceof Increment) t.increment((Increment) op);
		else if (op instanceof Append) t.append((Append) op);
	}

	public void put(String table, List<Row> ops) throws IOException {
		if (null == ops || ops.isEmpty()) return;
		Table t = table(table);
		Object[] results = new Object[ops.size()];
		try {
			t.batch(ops, results);
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	public <T> T table(String name, Function<Table, T> using) {
		return using.apply(table(name));
	}

	private final Statistic s = new Statistic(HbaseConnection.class).sizing(Result::getTotalSizeOfCells).step(GET_STATS_STEP).detailing(
			() -> "Cached scaner object: " + scans.size());

	public Rmap get(String table, Get get) {
		return table(table, t -> {
			Result r;
			try {
				r = t.get(get);
			} catch (Exception e) {
				logger().warn("Hbase scan fail: [" + e.getMessage() + "].");
				return null;
			}
			if (null != r) return Hbases.Results.result(table, r);
			logger().error("Hbase get/scan return null: \n\t" + get.toString());
			return null;
		});
	}

	public List<Rmap> get(String table, List<Get> gets) {
		if (gets == null || gets.isEmpty()) return Colls.list();
		if (gets.size() == 1) {
			Rmap r = Hbases.Results.result(table, s.stats(scan(table, gets.get(0))));
			return null == r ? Colls.list() : Colls.list(r);
		}
		List<Result> rr = s.statsIns(() -> table(table, t -> {
			try {
				return Colls.list(t.get(gets));
			} catch (IOException e) {
				return of(gets).map(g -> scan(table, g)).nonNull().list();
			}
		}));
		Sdream<Result> ss = of(rr);
		// if (GET_DUMP_MIN_SIZE > 0) ss = ss.peek(r -> dump(table, r));
		return ss.map(r -> Hbases.Results.result(table, r)).list();
	}

	public Result dump(String table, Result r, Function<byte[], Map<String, Object>> conv) {
		long size = Result.getTotalSizeOfCells(r);
		if (size < GET_DUMP_MIN_SIZE) return r;
		String fn = Texts.formatDate("hhMMssSSS", new Date()) + "-ROWKEY:" + Bytes.toString(r.getRow()) + "-SIZE:" + size;
		logger.error("Dump: " + fn);
		try (FileOutputStream fs = new FileOutputStream(fn + ".bin")) {
			fs.write(Hbases.toBytes(r));
		} catch (Exception e) {
			logger.error("Dump fail: " + e.getMessage());
		}
		Rmap m = Hbases.Results.result(table, r);
		try (PrintWriter w = new PrintWriter(new BufferedWriter(new FileWriter(fn + ".txt")));) {
			for (String key : m.keySet()) {
				Object val = m.get(key);
				w.println(key + ": ");
				w.println("\t" + (val instanceof byte[] ? conv.apply((byte[]) val).toString() : val.toString()));
			}
		} catch (Exception e) {
			logger.error("Dump fail: " + e.getMessage());
		}
		return r;
	}

	@Deprecated
	public List<Rmap> get1(String table, List<Get> gets) {
		if (gets == null || gets.isEmpty()) return Colls.list();
		if (gets.size() == 1) return Colls.list(Hbases.Results.result(table, s.stats(scan(table, gets.get(0)))));
		return table(table, t -> {
			LinkedBlockingQueue<Get> all = new LinkedBlockingQueue<>(gets);
			List<Callable<List<Rmap>>> tasks = Colls.list();
			while (!all.isEmpty()) {
				List<Get> batch = Colls.list();
				all.drainTo(batch, GET_BATCH_SIZE);
				tasks.add(() -> {
					try {
						return of(s.stats(Colls.list(t.get(batch)))).map(r -> Hbases.Results.result(table, r)).list();
					} catch (Exception ex) {
						return of(batch).map(g -> Hbases.Results.result(table, s.stats(scan(table, gets.get(0))))).nonNull().list();
					}
				});
			}
			return of(Exeter.of().join(tasks)).mapFlat(Sdream::of).list();
		});
	}

	private Result scan(String table, Get get) {
		byte[] row = get.getRow();
		return table(table, t -> {
			int retry = 0;
			long now = System.currentTimeMillis();
			Result r = null;
			do {
				try {
					Scan s = scanOpen(row, get.getFilter(), get.familySet().toArray(new byte[get.numFamilies()][]));
					try (ResultScanner sc = t.getScanner(s);) {
						r = sc.next();
					} finally {
						scanClose(s);
					}
				} catch (Exception ex) {
					if (doNotRetry(ex)) {
						logger().error("Hbase get(scan) failed on retry #" + retry + ": [" + Bytes.toString(row) + "] in [" + (System
								.currentTimeMillis() - now) + " ms], error:\n\t" + ex.getMessage());
						return null;
					}
				}
			} while (null == r && retry++ < GET_MAX_RETRIES);
			if (null == r) return null;
			Result rr = r;
			int rt = retry;
			logger().trace(() -> "Hbase get(scan) on [" + Bytes.toString(row) + "] with [" + rt + "] retries, size: [" + Result
					.getTotalSizeOfCells(rr) + "]");
			return r;
		});
	}

	private boolean doNotRetry(Throwable th) {
		while (!(th instanceof RemoteWithExtrasException) && th.getCause() != null && th.getCause() != th)
			th = th.getCause();
		if (th instanceof RemoteWithExtrasException) return ((RemoteWithExtrasException) th).isDoNotRetry();
		else return true;
	}

	private Scan scanOpen(byte[] row, Filter filter, byte[]... families) {
		Scan s = Hbases.optimize(new Scan(), GET_SCAN_LIMIT, GET_SCAN_LIMIT, GET_SCAN_BYTES);
		s = s.setStartRow(row).setStopRow(row);
		s.getFamilyMap().clear();
		for (byte[] cf : families)
			s.addFamily(cf);
		s.setFilter(filter);
		return s;
	}

	private void scanClose(Scan s) {
		// scans.offer(s);
	}

	public static class Driver implements com.hzcominfo.albatis.nosql.Connection.Driver<HbaseConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public HbaseConnection connect(URISpec uriSpec) throws IOException {
			return new HbaseConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("hbase");
		}
	}

	@Override
	public HbaseInput input(TableDesc... tables) throws IOException {
		HbaseInput input = new HbaseInput("HbaseInput", this);
		for (TableDesc table : tables) {
			String tableName = table.name;
			String[] fields = Arrays.asList(table.fields()).stream().map(f -> f.name).collect(Collectors.toList()).toArray(new String[] {});
			input.tableWithFamily(tableName, fields);
		}
		return input;
	}

	@Override
	public HbaseOutput output(TableDesc... table) throws IOException {
		return new HbaseOutput("HbaseOutput", this, conv);
	}

	/**
	 * hbase create table
	 *
	 * @param url
	 * @param table
	 * @param tableCustomSet
	 */
	public void createHbaseTable(String url, String table, TableCustomSet tableCustomSet) {
		try {
			JSONObject regions = JSONObject.parseObject(JSON.toJSONString(tableCustomSet.getOptions().get("regions")), JSONObject.class);
			List<String> columnFamilies = com.alibaba.fastjson.JSONArray.parseArray(JSON.toJSONString(regions.get("columnFamilies")), String.class);
			Integer region = (Integer) regions.get("region");
			if (null == region) {
				createHbaseTable(url, table, columnFamilies);
			} else if (region < 3) {
				createHbaseTable(url, table, columnFamilies);
			} else {
				byte[] startKeys = JSON.toJSONString(regions.get("start_key")).getBytes();
				byte[] endKeys = JSON.toJSONString(regions.get("end_key")).getBytes();
				createHbaseTable(url, table, startKeys, endKeys, region, columnFamilies);
			}
		} catch (IOException e) {
			throw new RuntimeException("create hbase table failure   " + e);
		}
	}

	public static void createHbaseTable(String url, String tableName, byte[] startKey, byte[] endKey, Integer numRegions, List<String> columnFamilies) throws IOException {
		HbaseConnection hbaseConnection = new HbaseConnection(new URISpec(url));
		org.apache.hadoop.hbase.client.Connection client = hbaseConnection.client;
		Admin admin = client.getAdmin();
		// 表名
		TableName tn = TableName.valueOf(tableName);
		HTableDescriptor table = new HTableDescriptor(tn);
		for (String family : columnFamilies) {
			// 列族的名字
			HColumnDescriptor columnFamily = new HColumnDescriptor(family);
			table.addFamily(columnFamily);
		}
		// 创建表
		if (startKey != null && endKey != null) {
			admin.createTable(table, startKey, endKey, numRegions);
		} else {
			admin.createTable(table);
		}
		admin.close();
		client.close();
		hbaseConnection.close();
	}

	public static void createHbaseTable(String url, String tableName, List<String> columnFamilies) throws IOException {
		createHbaseTable(url, tableName, null, null, null, columnFamilies);
	}

	// 服务器Hbase版本1.2.0-cdh5.15.1 本地客户端降级

	/**
	 * judge hbase whether create table
	 *
	 * @param url
	 * @param table
	 * @return
	 */
	public boolean judgeHbase(String url, String table) {
		boolean exists = false;
		try (HbaseConnection hbaseConnection = new HbaseConnection(new URISpec(url));
				org.apache.hadoop.hbase.client.Connection client = hbaseConnection.client;
				Admin admin = client.getAdmin()) {
			exists = admin.tableExists(TableName.valueOf(table));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return exists;
	}

}
