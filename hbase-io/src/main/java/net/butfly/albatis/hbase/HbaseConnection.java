package net.butfly.albatis.hbase;

import static net.butfly.albacore.paral.Sdream.of;
import static net.butfly.albacore.utils.collection.Colls.empty;
import static net.butfly.albacore.utils.collection.Colls.list;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_ZWNJ;
import static net.butfly.albatis.io.IOProps.propI;
import static net.butfly.albatis.io.IOProps.propL;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.ipc.RemoteWithExtrasException;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.Builder;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.IOFactory;
import net.butfly.albatis.io.IOStats;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.utils.JsonUtils;
import net.butfly.alserdes.SerDes;

@SerDes.As("hbase")
public class HbaseConnection extends DataConnection<org.apache.hadoop.hbase.client.Connection> implements IOFactory, IOStats {
	protected final static Logger logger = Logger.getLogger(HbaseConnection.class);

	static {
		logger.warn("Hbase client common lib not support 2 digitals jdk version\n\t"//
				+ "(checked by \"\\d\\.\\d\\..*\" in org.apache.hadoop.hbase.util.ClassSize:118)\nhacking into 9.0.4 -_-");
		System.setProperty("java.version", "9.0.4");
		// XXX: check
		ClassSize.align(ClassSize.OBJECT + 2 * ClassSize.REFERENCE + 1 * Bytes.SIZEOF_LONG + ClassSize.REFERENCE + ClassSize.REFERENCE
				+ ClassSize.TREEMAP);
	}

	private final int GET_SCAN_OBJS = propI(this, "get.scaner.queue", 500);
	private final int GET_MAX_RETRIES = propI(this, "get.retry", 2);
	private final int GET_SCAN_BYTES = propI(this, "get.result.bytes", 3145728); // 3M
	private final int GET_SCAN_LIMIT = propI(this, "get.result.limit", 1);
	protected final long GET_DUMP_MIN_SIZE = propL(this, "get.dump.min.bytes", 2097152); // 2M
	private final Map<String, Table> tables;
	private final LinkedBlockingQueue<Scan> scans = new LinkedBlockingQueue<>(GET_SCAN_OBJS);

	public HbaseConnection() throws IOException {
		this(new URISpec("hbase:///"));
	}

	public HbaseConnection(URISpec uri) throws IOException {
		super(uri, "hbase");
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
			boolean configed = false;
			params = new ConcurrentHashMap<>(uri.getParameters());
			// params.remove("df");
			switch (uri.getScheme()) {
			case "hbase:master":
				if (uri.getInetAddrs().length == 1) {
					logger.warn("Deprecate master connect to hbase: " + uri.getHost());
					params.put("hbase.master", "*" + uri.getHost() + "*");
					configed = true;
					break;
				}
			case "hbase":
			case "zk":
			case "zookeeper":
			case "hbase:zk":
			case "hbase:zookeeper":
				if (uri.getInetAddrs().length > 0) {
					for (InetSocketAddress a : uri.getInetAddrs()) {
						params.put(HConstants.ZOOKEEPER_QUORUM, a.getHostName());
						params.put(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(a.getPort()));
					}
					if (!"/".equals(uri.getPath())) params.put(HConstants.ZOOKEEPER_ZNODE_PARENT, uri.getPath());
					configed = true;
					break;
				}
			}
			if (!configed) configurateByFile(params, uri.getPath());
		}
		try {
			return Hbases.connect(params);
		} catch (IOException e) {
			logger.error("Connect failed", e);
			return null;
		}
	}

	private void configurateByFile(Map<String, String> params, String path) {
		if (!path.endsWith("/")) throw new IllegalArgumentException("Hbase configuration should be a directory, but a file [" + path
				+ "] is found.");
		if ("/".equals(path)) return;// empty, classpath configuration files.
		char prefix = path.charAt(1);
		if (prefix == '.' || prefix == '~') path = path.substring(1);// relative path
		params.put("albatis.hbase.config.path", path);
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
		if (empty(ops)) return;
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

	private final Statistic s = stating().detailing(() -> "Cached scaner object: " + scans.size()).sizing(Result::getTotalSizeOfCells);

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
		if (empty(gets)) return list();
		if (gets.size() == 1) {
			Rmap r = Hbases.Results.result(table, s.stats(scan(table, gets.get(0))));
			return null == r ? list() : list(r);
		}
		List<Result> rr = s.statsIns(() -> table(table, t -> {
			try {
				return list(t.get(gets));
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
		if (empty(gets)) return list();
		if (gets.size() == 1) return list(Hbases.Results.result(table, s.stats(scan(table, gets.get(0)))));
		return table(table, t -> {
			LinkedBlockingQueue<Get> all = new LinkedBlockingQueue<>(gets);
			List<Callable<List<Rmap>>> tasks = list();
			while (!all.isEmpty()) {
				List<Get> batch = list();
				all.drainTo(batch, batchSize());
				tasks.add(() -> {
					try {
						return of(s.stats(list(t.get(batch)))).map(r -> Hbases.Results.result(table, r)).list();
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

	public static class Driver implements net.butfly.albatis.Connection.Driver<HbaseConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public HbaseConnection connect(URISpec uriSpec) throws IOException {
			return new HbaseConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return list("hbase", "hbase:zk");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public HbaseInput inputRaw(TableDesc... tables) throws IOException {
		HbaseInput input = new HbaseInput("HbaseInput", this);
		for (TableDesc table : tables) {
			String[] fqs = Builder.parseTableAndField(table.name, null);
			List<String> pfxs = null == fqs[2] ? list() : list(p -> "".equals(p) ? null : p, fqs[2].split(SPLIT_ZWNJ));
			List<String> cfs = null == fqs[1] ? list() : list(p -> "".equals(p) ? null : p, fqs[1].split(SPLIT_ZWNJ));
			fqs = fqs[0].split(SPLIT_ZWNJ);
			if (fqs.length > 1) {// row key mode
				List<byte[]> rows = list(Bytes::toBytes, fqs);
				rows.remove(0);
				input.table(fqs[0], table.name, rows.toArray(new byte[rows.size()][]));
			} else // input.table(fqs[0]); // debug
			input.tableWithFamilAndPrefix(fqs[0], pfxs, cfs.toArray(new String[0]));
		}
		return input;
	}

	@SuppressWarnings("unchecked")
	@Override
	public HbaseOutput outputRaw(TableDesc... table) throws IOException {
		return new HbaseOutput("HbaseOutput", this);
	}

	@Override
	public void construct(String table, TableDesc tableDesc, List<FieldDesc> fields) {
		try {
			String[] tables = table.split("\\.");
			String tableName;
			if (tables.length == 1) tableName = tables[0];
			else if ((tables.length == 2)) tableName = tables[1];
			else throw new RuntimeException("Please type in corrent es table format: db.table !");
			Object families = tableDesc.construct.get("columnFamilies");
			Object startKey = tableDesc.construct.get("start_key");
			Object endKey = tableDesc.construct.get("end_key");
			Object splitKey = tableDesc.construct.get("region_split_keys");
			Object region = tableDesc.construct.get("region");
			createHbaseTable(client, tableName, startKey, endKey, splitKey, region, families);
		} catch (IOException e) {
			throw new RuntimeException("create hbase table failure   ", e);
		}
	}

	public static void createHbaseTable(Connection client, String tableName, Object startKey, Object endKey, Object splitKey,
			Object numRegions, Object families) throws IOException {
		byte[] startKeys = null;
		byte[] endKeys = null;
		byte[][] splitKeys = null;
		String s = null;
		if (null != startKey) startKeys = JsonUtils.stringify(startKey).getBytes();
		if (null != endKey) endKeys = JsonUtils.stringify(endKey).getBytes();
		if (null != splitKey) {
			if (splitKey instanceof String) s = (String) splitKey;
			else s = JsonUtils.stringify(splitKey);
			if (null != s) {
				String[] splitString = s.split(",");
				splitKeys = new byte[splitString.length][];
				int tmpIndex = 0;
				for (String split : splitString) {
					splitKeys[tmpIndex] = split.getBytes();
					++tmpIndex;
				}
			}
		}
		Admin admin = client.getAdmin();
		// 表名
		TableName tn = TableName.valueOf(tableName);
		HTableDescriptor table = new HTableDescriptor(tn);
		// [cf1,cf2]
		List<String> columnFamilies = Colls.list();
		if (families instanceof List) {
			columnFamilies = JsonUtils.parseFieldsByJson(families);
			for (String family : columnFamilies) {
				// 列族的名字
				HColumnDescriptor columnFamily = new HColumnDescriptor(family);
				table.addFamily(columnFamily);
			}
		}
		Map<String, Object> familyMap = new HashMap<>();
		// {"cf1":{"compression":"SNAPPY","versions":3},"cf2":{"compression":"SNAPPY","versions":3}}
		if (families instanceof Map) {
			familyMap = JsonUtils.parseObject2Map(families);
			familyMap.forEach((k, v) -> {
				HColumnDescriptor columnFamily = new HColumnDescriptor(k);
				Map<String, Object> familyParam = JsonUtils.parseObject2Map(v);
				if (null != familyParam.get("min_versions")) {
					int minVersion = Integer.parseInt(familyParam.get("min_versions").toString());
					columnFamily.setMinVersions(minVersion);
				}
				if (null != familyParam.get("max_versions")) {
					int maxVersion = Integer.parseInt(familyParam.get("max_versions").toString());
					columnFamily.setMaxVersions(maxVersion);
				}
				if (null != familyParam.get("compression")) {
					String compression = String.valueOf(familyParam.get("compression")).toUpperCase();
					columnFamily.setCompressionType(Compression.Algorithm.valueOf(compression));
				}
				if (null != familyParam.get("bloom_filter_type")) {
					BloomType bloomFilterType = BloomType.valueOf(familyParam.get("bloom_filter_type").toString().toUpperCase());
					columnFamily.setBloomFilterType(bloomFilterType);
				}
				if (null != familyParam.get("in_memory")) {
					Boolean inMemory = Boolean.valueOf(familyParam.get("in_memory").toString());
					columnFamily.setInMemory(inMemory);
				}
				if (null != familyParam.get("keep_deleted_cells")) {
					KeepDeletedCells keepDeletedCells = KeepDeletedCells.valueOf(familyParam.get("keep_deleted_cells").toString()
							.toUpperCase());
					columnFamily.setKeepDeletedCells(keepDeletedCells);
				}
				if (null != familyParam.get("data_block_encoding")) {
					DataBlockEncoding dataBlockEncoding = DataBlockEncoding.valueOf(familyParam.get("data_block_encoding").toString()
							.toUpperCase());
					columnFamily.setDataBlockEncoding(dataBlockEncoding);
				}
				if (null != familyParam.get("ttl")) {
					int ttl = Integer.parseInt(familyParam.get("ttl").toString());
					columnFamily.setTimeToLive(ttl);
				}
				if (null != familyParam.get("block_cache")) {
					Boolean blockCache = Boolean.valueOf(familyParam.get("block_cache").toString());
					columnFamily.setBlockCacheEnabled(blockCache);
				}
				if (null != familyParam.get("block_size")) {
					int blockSize = Integer.parseInt(familyParam.get("block_size").toString());
					columnFamily.setBlocksize(blockSize);
				}
				if (null != familyParam.get("replication_scope")) {
					int scope = Integer.parseInt(familyParam.get("replication_scope").toString());
					columnFamily.setScope(scope);
				}
				table.addFamily(columnFamily);
			});
		}
		// 创建表
		if (null != splitKey) admin.createTable(table, splitKeys);
		else if (null != startKey && null != endKey && null != numRegions) {
			if ((Integer) numRegions > 3) {
				admin.createTable(table, startKeys, endKeys, (Integer) numRegions);
			} else if ((Integer) numRegions == 3) admin.createTable(table, new byte[][] { startKeys, endKeys });
			else throw new IllegalArgumentException("Must create at least three regions");
		} else {
			admin.createTable(table);
		}
	}

	@Override
	public boolean judge(String table) {
		boolean exists = false;
		String[] tables = table.split("\\.");
		String tableName;
		if (tables.length == 1) tableName = tables[0];
		else if ((tables.length == 2)) tableName = tables[1];
		else throw new RuntimeException("Please type in corrent es table format: db.table !");
		try (Admin admin = client.getAdmin()) {
			exists = admin.tableExists(TableName.valueOf(tableName));
		} catch (IOException e) {
			logger().error("hbase judge table isExists error", e);
		}
		return exists;
	}

}
