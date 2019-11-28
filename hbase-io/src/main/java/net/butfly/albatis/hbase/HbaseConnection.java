package net.butfly.albatis.hbase;

import static net.butfly.albacore.paral.Sdream.of;
import static net.butfly.albacore.utils.collection.Colls.empty;
import static net.butfly.albacore.utils.collection.Colls.list;
import static net.butfly.albatis.hbase.HbaseInput.SCAN_BYTES;
import static net.butfly.albatis.hbase.HbaseInput.SCAN_CACHE_BLOCKS;
import static net.butfly.albatis.hbase.HbaseInput.SCAN_COLS;
import static net.butfly.albatis.hbase.utils.HbaseScan.ROWKEY_UNDEFINED;
import static net.butfly.albatis.hbase.utils.HbaseScan.Options.opts;
import static net.butfly.albatis.hbase.utils.HbaseScan.Range.range;
import static net.butfly.albatis.hbase.utils.Hbases.Results.result;
import static net.butfly.albatis.io.IOProps.propB;
import static net.butfly.albatis.io.IOProps.propI;
import static net.butfly.albatis.io.IOProps.propL;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
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
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.ipc.RemoteWithExtrasException;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.hbase.util.ClassSize;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.hbase.utils.HbaseScan.Range;
import net.butfly.albatis.hbase.utils.HbaseSkip;
import net.butfly.albatis.hbase.utils.HbaseSkip.SkipMode;
import net.butfly.albatis.hbase.utils.Hbases;
import net.butfly.albatis.hbase.utils.ZkUtils;
import net.butfly.albatis.io.IOFactory;
import net.butfly.albatis.io.IOStats;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.utils.JsonUtils;
import net.butfly.albatis.io.vfs.VfsConnection;
import net.butfly.alserdes.SerDes;

@SuppressWarnings("deprecation")
@SerDes.As("hbase")
public class HbaseConnection extends DataConnection<org.apache.hadoop.hbase.client.Connection> implements IOFactory, IOStats {
	protected final static Logger logger = Logger.getLogger(HbaseConnection.class);
	private static final String VFS_CONF_URI = "albatis.hbase.config.path";

	// static {
	// logger.warn("Hbase client common lib not support 2 digitals jdk version\n\t"//
	// + "(checked by \"\\d\\.\\d\\..*\" in org.apache.hadoop.hbase.util.ClassSize:118)\nhacking into 9.0.4 -_-");
	// System.setProperty("java.version", "9.0.4");
	// // XXX: check
	// ClassSize.align(ClassSize.OBJECT + 2 * ClassSize.REFERENCE + 1 * Bytes.SIZEOF_LONG + ClassSize.REFERENCE + ClassSize.REFERENCE
	// + ClassSize.TREEMAP);
	// }

	final boolean OP_SINGLE = propB(this, "use.single.op", false);
	private final boolean OP_CONFIRM_RETRY = propB(this, "confirm.and.retry", false);
	// private final int GET_SCAN_OBJS = propI(this, "get.scaner.object.queue.size", 500);
	private final int GET_MAX_RETRIES = propI(this, "get.retry", 2);
	protected final long GET_DUMP_MIN_SIZE = propL(this, "get.dump.min.bytes", 2097152); // 2M
	private final Map<String, Table> tables;
	private String defaultNamespace;
	// private final LinkedBlockingQueue<Scan> scans = new LinkedBlockingQueue<>(GET_SCAN_OBJS);
	// RmapSubMode subtableMode;

	public HbaseConnection() throws IOException {
		this(new URISpec("hbase:///"));
	}

	public HbaseConnection(URISpec uri) throws IOException {
		super(uri, "hbase");
		tables = Maps.of();
	}

	@Override
	protected org.apache.hadoop.hbase.client.Connection initialize(URISpec uri) {
		// this.subtableMode = RmapSubMode.valueOf(uri.fetchParameter("sub", "NONE").toUpperCase());
		if (null != client) try {
			client.close();
		} catch (Exception e) {
			logger.error("Initialize hbase connection fail on close origin, maybe leak!", e);
		}

		Map<String, String> params = params();
		if (defaultNamespace == null) {
			String fullPath = uri.getPath();
			if (fullPath.startsWith("/")) fullPath = fullPath.substring(1);
			if (fullPath.endsWith("/")) fullPath = fullPath.substring(0, fullPath.length() - 1);
			String[] paths = fullPath.split("/");
			defaultNamespace = paths == null || paths[paths.length - 1] == null || paths[paths.length - 1].isEmpty() ? null : paths[paths.length - 1];
		}
		params.putAll(uri.getParameters());
		String confuri = params.remove(VFS_CONF_URI);
		if (null == confuri) try {
			return Hbases.connect(params);
		} catch (IOException e) {
			logger.error("Connect failed", e);
			return null;
		}
		else try {
			return Hbases.connect(params, VfsConnection.readAll(confuri));
		} catch (IOException e) {
			logger.error("Connect failed", e);
			return null;
		}
	}


	private Map<String, String> params() {
		if (null != uri) {
			// params.remove("df");
//			defaultNamespace = uri.fetchParameter("namespace");
			String[] s = uri.getSchemas();
			int i = 0;
			if ("hbase".equals(s[0]) && s.length > 1) i = 1;
			switch (s[i]) {
			case "master": // deprecated single master mode
				if (uri.getInetAddrs().length == 1) {
					logger.warn("Deprecate master connect to hbase: " + uri.getHost());
					return Maps.of("hbase.master", "*" + uri.getHost() + "*");
				} else throw new IllegalArgumentException("Hbase master mode need host and port.");
			case "hbase":
			case "zk":
			case "zookeeper": // default zookeeper mode
				Map<String, String> params = Maps.of();
				if (uri.getInetAddrs().length > 0) {
					StringBuilder zq = new StringBuilder();
					int port = -1;
					for (InetSocketAddress a : uri.getInetAddrs()) {
						zq.append(a.getHostName()).append(",");
						if (port > 0 && port != a.getPort()) //
							throw new IllegalArgumentException("Hbase zookeeper port conflicted " + port + " and " + a.getPort());
						port = a.getPort();
					}
					params.put(HConstants.ZOOKEEPER_QUORUM, zq.toString());
					if (port > 0) params.put(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(port));

					Pair<String, String> pathAndNamespace = ZkUtils.getHbaseZkPath(uri.getHost(), uri.getPath());
					if (null != pathAndNamespace) {
						defaultNamespace = pathAndNamespace.v2();
						if (!"/".equals(pathAndNamespace.v1())) params.put(HConstants.ZOOKEEPER_ZNODE_PARENT, pathAndNamespace.v1());
					}
				}
				return params;
			default: // other, try to load by vfs
				URISpec vfs = uri.schema(Arrays.copyOfRange(s, i, s.length));
				String vfsUri = vfs.toString();
				return Maps.of(VFS_CONF_URI, vfsUri.indexOf("?") == -1 ? vfsUri : vfsUri.substring(0, vfsUri.indexOf("?")));
			}
		}
		throw new IllegalArgumentException("Hbase uri not supported.");
	}

	@Override
	public void close() throws IOException {
		super.close();
		for (String k : tables.keySet()) try {
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

	private static final AtomicInteger C = new AtomicInteger();

	public Result put(String table, Mutation op) throws IOException {
		Table t = table(table);
		put1(t, op);
		Result r = check(t, op);
		return r;
	}

	public Result[] put(String table, Mutation... ops) throws IOException {
		// int cc = C.incrementAndGet();
		Table t = table(table);
		Object[] results = new Object[ops.length];
		try {
			t.batch(Arrays.asList(ops), results);
			// } catch (IOException e) {
			// logger.error("Hbase IO failed", e);
		} catch (InterruptedException e) {
			throw new RuntimeException();
		}
		Result[] rs = new Result[ops.length];
		for (int i = 0; i < ops.length; i++) {
			Object r = results[i];
			if (null == r) rs[i] = null;
			else if (r instanceof Result) {
				if (invalid(ops[i], (Result) r)) rs[i] = check(t, ops[i]);
				if (null == rs[i]) rs[i] = (Result) r;
			} else logger.error("Unknown error result from batch of [" + i + "][" + ops[i].toJSON() + "]: " + r.toString());
		}
		// if (logger().isTraceEnabled()) for (int i = 0; i < ops.length; i++) try {
		// Mutation op = ops[i];
		// logger().trace("[enqs:" + op.getClass().getSimpleName() + ":" + cc + ":" + (i + 1) + "]\t" + op.toJSON() //
		// + "\n\tResult: " + String.valueOf(rs[i]));
		// } catch (IOException e) {}
		return rs;
	}

	private Result check(Table t, Mutation op) throws IOException {
		if (OP_CONFIRM_RETRY) {
			Result r;
			while (invalid(op, r = t.get(new Get(op.getRow())))) {
				logger().warn("Retry [" + op.toJSON() + "] for rowkey [" + Bytes.toString(op.getRow()) + "] not found after op: " + r
						.toString());
				put1(t, op);
			}
			logger().trace("[" + op.getClass().getSimpleName() + ":" + C.incrementAndGet() + "]\t" + op.toJSON() //
					+ "\n\tResult: " + String.valueOf(r));
			return r;
		} else return null;
	}

	private boolean invalid(Mutation op, Result r) {
		return (op instanceof Delete && null != r && null != r.getRow()) || (null == r || null == r.getRow());
	}

	private static void put1(Table t, Mutation op) throws IOException {
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

	private final Statistic s = null;// s().detailing(() -> "Cached scaner object: " + scans.size()).sizing(Result::getTotalSizeOfCells);

	public Rmap get(String table, Get get) {
		return table(table, t -> {
			Result r;
			try {
				r = t.get(get);
			} catch (Exception e) {
				logger().warn("Hbase scan fail: [" + e.getMessage() + "].");
				return null;
			}
			if (null != r) return result(table, r);
			logger().error("Hbase get/scan return null: \n\t" + get.toString());
			return null;
		});
	}

	public List<Rmap> get(String table, List<Get> gets) {
		if (empty(gets)) return list();
		if (gets.size() == 1) {
			Result rr = scan(table, gets.get(0));
			rr = null == s ? rr : s.stats(rr);
			Rmap r = result(table, rr);
			return null == r ? list() : list(r);
		}
		List<Result> rr;
		if (null == s) try {
			rr = list(table(table).get(gets));
		} catch (IOException e1) {
			rr = of(gets).map(g -> scan(table, g)).nonNull().list();
		}
		else rr = s.statsIns(() -> table(table, t -> {
			try {
				return list(t.get(gets));
			} catch (IOException e) {
				return of(gets).map(g -> scan(table, g)).nonNull().list();
			}
		}));
		Sdream<Result> ss = of(rr);
		// if (GET_DUMP_MIN_SIZE > 0) ss = ss.peek(r -> dump(table, r));
		return ss.map(r -> result(table, r)).list();
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
		Rmap m = result(table, r);
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
		if (gets.size() == 1) return list(result(table, s.stats(scan(table, gets.get(0)))));
		return table(table, t -> {
			LinkedBlockingQueue<Get> all = new LinkedBlockingQueue<>(gets);
			List<Callable<List<Rmap>>> tasks = list();
			while (!all.isEmpty()) {
				List<Get> batch = list();
				all.drainTo(batch, batchSize());
				tasks.add(() -> {
					try {
						return of(s.stats(list(t.get(batch)))).map(r -> result(table, r)).list();
					} catch (Exception ex) {
						return of(batch).map(g -> result(table, s.stats(scan(table, gets.get(0))))).nonNull().list();
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
					Scan s = opts(1, SCAN_COLS, SCAN_BYTES, SCAN_CACHE_BLOCKS).optimize(range(row).scan(get.getFilter()));
					s.getFamilyMap().clear();
					for (byte[] cf : get.familySet()) s.addFamily(cf);
					try (ResultScanner sc = t.getScanner(s);) {
						r = sc.next();
					}
				} catch (Exception ex) {
					if (doNotRetry(ex)) {
						logger().error("Hbase get(scan) failed on retry #" + retry + ": [" + Bytes.toString(row) + "] in [" //
								+ (System.currentTimeMillis() - now) + " ms], error:\n\t" + ex.getMessage());
						return null;
					}
				}
			} while (null == r && retry++ < GET_MAX_RETRIES);
			if (null == r) return null;
			Result rr = r;
			int rt = retry;
			logger().trace(() -> "Hbase get(scan) on [" + Bytes.toString(row) + "] with [" + rt + "] retries, size: [" //
					+ Result.getTotalSizeOfCells(rr) + "]");
			return r;
		});
	}

	private boolean doNotRetry(Throwable th) {
		while (!(th instanceof RemoteWithExtrasException) && th.getCause() != null && th.getCause() != th) th = th.getCause();
		if (th instanceof RemoteWithExtrasException) return ((RemoteWithExtrasException) th).isDoNotRetry();
		else return true;
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
		Map<String, Pair<Set<String>, Set<String>>> tbls = Maps.of();
		Map<String, String> rowkeys = Maps.of();
		for (TableDesc table : tables) {
			Pair<Set<String>, Set<String>> cfAndPfxList = tbls.computeIfAbsent(table.qualifier.name, n -> new Pair<>(new HashSet<>(),
					new HashSet<>()));
			if (null != table.qualifier.family) cfAndPfxList.v1().add(table.qualifier.family);
			if (null != table.qualifier.prefix) cfAndPfxList.v2().add(table.qualifier.prefix);
			String rowkey = table.attr("..ROW_KEY_FILTER", String.class);
			if (null != rowkey) rowkeys.put(table.qualifier.name, rowkey);
		}
		String rowkey;
		for (String t : tbls.keySet()) if (null != (rowkey = rowkeys.get(t))) // row key mode
			input.table(defaultNamespace, t, tbls.get(t).v1(), tbls.get(t).v2(), Bytes.toBytes(rowkey));
		else input.table(defaultNamespace, t, tbls.get(t).v1(), tbls.get(t).v2());
		return input;
	}

	@SuppressWarnings("unchecked")
	@Override
	public HbaseOutput outputRaw(TableDesc... table) throws IOException {
		return new HbaseOutput("HbaseOutput", defaultNamespace, this);
	}

	@Override
	public void construct(String table, TableDesc tableDesc, List<FieldDesc> fields) {
		try {
			String tableName;
			String[] tables = table.split("\\.");
			if (tables.length == 1) tableName = tables[0];
			else if (tables.length == 2) tableName = tables[1];
			else throw new RuntimeException("Please type in correct hbase table format: db.table !");
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

	public static void createHbaseTable(Connection client, String tableName, Object startKey, Object endKey, Object splitKey, Object numRegions,
			Object families) throws IOException {
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
					KeepDeletedCells keepDeletedCells = KeepDeletedCells.valueOf(//
							familyParam.get("keep_deleted_cells").toString().toUpperCase());
					columnFamily.setKeepDeletedCells(keepDeletedCells);
				}
				if (null != familyParam.get("data_block_encoding")) {
					DataBlockEncoding dataBlockEncoding = DataBlockEncoding.valueOf(//
							familyParam.get("data_block_encoding").toString().toUpperCase());
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
		String tableName;
		boolean exists = false;
		String[] tables = table.split("\\.");
		if (tables.length == 1) tableName = tables[0];
		else if (tables.length == 2) tableName = tables[1];
		else throw new RuntimeException("Please type in correct hbase table format: db.table !");
		try (Admin admin = client.getAdmin()) {
			exists = admin.tableExists(TableName.valueOf(tableName));
		} catch (IOException e) {
			logger().error("hbase judge table isExists error", e);
		}
		return exists;
	}

	public List<String> ls() throws IOException {
		try (Admin admin = client.getAdmin()) {
			return list(tn -> tn.getNameAsString(), admin.listTableNames());
		}
	}

	public Range[] ranges(String table) {
		StringBuilder sb = new StringBuilder("Hbase region found:");
		try (RegionLocator rl = client.getRegionLocator(TableName.valueOf(table));) {
			List<Range> ranges = Colls.list(rl.getAllRegionLocations(), r -> {
				sb.append("\n\t").append(r.toString());
				HRegionInfo ri = r.getRegionInfo();
				return range(ri.getStartKey(), ri.getEndKey());
			});
			if (!ranges.isEmpty()) logger.debug(sb.toString());
			return ranges.toArray(new Range[0]);
		} catch (IOException e) {
			logger.warn("Rengions analyze for table [" + table + "] fail: " + e.getMessage());
			return null;
		}
	}

	/**
	 * alter 'subject_person', METHOD => 'table_att','coprocessor'=>'|org.apache.hadoop.hbase.coprocessor.AggregateImplementation||'
	 */
	public long countRows(String table, byte[]... range) {
		Scan scan = range(range).scan();
		try (AggregationClient ac = new AggregationClient(client.getConfiguration());) {
			return ac.rowCount(TableName.valueOf(table), new LongColumnInterpreter(), scan);
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}

	public byte[] skip(String table, SkipMode mode, String skip, Filter f) {
		try {
			switch (mode) {
			case ROWKEY:
				return Bytes.toBytes(skip);
			case REGIONS:
				return new HbaseSkip(this).skipByRegionNum(table, Integer.parseInt(skip), null);
			case ROWS:
				return new HbaseSkip(this).skipByScan(table, Long.parseLong(skip), null);
			case REGION_COUNT:
				return new HbaseSkip(this).skipByRegionCount(table, Long.parseLong(skip), null);
			default:
				return ROWKEY_UNDEFINED;
			}
		} catch (IOException e) {
			if (logger.isDebugEnabled()) logger.warn("Skip fail: ", e);
			else logger.warn("Skip fail: " + e.getMessage());
			return ROWKEY_UNDEFINED;
		}
	}
}
