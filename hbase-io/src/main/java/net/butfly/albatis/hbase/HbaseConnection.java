package net.butfly.albatis.hbase;

import static net.butfly.albacore.utils.collection.Streams.list;
import static net.butfly.albacore.utils.collection.Streams.of;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;

import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.io.utils.URISpec;
import net.butfly.albacore.utils.logger.Logger;

public class HbaseConnection extends NoSqlConnection<Connection> {
	private static final Logger logger = Logger.getLogger(HbaseConnection.class);

	public HbaseConnection(URISpec uriSpec) throws IOException {
		super(uriSpec, u -> {
			try {
				return Hbases.connect(params);
			} catch (IOException e) {
				return null;
			}
		}, "hbase");
		tables = new ConcurrentHashMap<>();
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
		try {
			synchronized (this) {
				if (!client().isClosed()) client().close();
			}
		} catch (IOException e) {
			logger().error("Hbase close failure", e);
		}
	}

	public Message get(String table, Get get) {
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

	public Table table(String table) {
		return tables.computeIfAbsent(table, t -> {
			try {
				return client().getTable(TableName.valueOf(t), Hbases.ex);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
	}

	public <T> T table(String name, Function<Table, T> using) {
		return using.apply(table(name));
	}

	public List<Message> get(String table, List<Get> gets) {
		if (gets == null || gets.isEmpty()) return new ArrayList<>();
		if (gets.size() == 1) return Arrays.asList(scan(table, gets.get(0)));
		return table(table, t -> {
			try {
				return list(Arrays.asList(t.get(gets)), r -> Hbases.Results.result(table, r));
			} catch (Exception ex) {
				return list(of(gets, true).map(g -> scan(table, g)).filter(r -> null != r));
			}
		});
	}

	private Message scan(String table, Get get) {
		byte[] row = get.getRow();
		return table(table, t -> {
			int retry = 0;
			long now = System.currentTimeMillis();
			Result r = null;
			do {
				Scan s = scanOpen(row, get.getFilter(), get.familySet().toArray(new byte[get.numFamilies()][]));
				try (ResultScanner sc = t.getScanner(s);) {
					r = sc.next();
				} catch (IOException ex) {
					if (doNotRetry(ex)) {
						logger().error("Hbase get(scan) failed on retry #" + retry + ": [" + Bytes.toString(row) + "] in [" + (System
								.currentTimeMillis() - now) + " ms], error:\n\t" + ex.getMessage());
						return null;
					}
				} finally {
					scanClose(s);
				}
			} while (null == r && retry++ < MAX_RETRIES);

			if (null == r) return null;
			Result rr = r;
			int rt = retry;
			logger().trace(() -> "Hbase get(scan) on [" + Bytes.toString(row) + "] with [" + rt + "] retries, size: [" + Result
					.getTotalSizeOfCells(rr) + "]");
			return Hbases.Results.result(table, r);
		});
	}

	private boolean doNotRetry(Throwable th) {
		while (!(th instanceof RemoteWithExtrasException) && th.getCause() != null && th.getCause() != th)
			th = th.getCause();
		if (th instanceof RemoteWithExtrasException) return ((RemoteWithExtrasException) th).isDoNotRetry();
		else return true;
	}

	private Scan scanOpen(byte[] row, Filter filter, byte[]... families) {
		Scan s = scans.poll();
		if (null == s) {
			s = new Scan();
			optimize(s);
		}
		s = s.setStartRow(row).setStopRow(row);
		s.getFamilyMap().clear();
		for (byte[] cf : families)
			s.addFamily(cf);
		s.setFilter(filter);
		return s;
	}

	private void scanClose(Scan s) {
		scans.offer(s);
	}

	private static final int HBASE_SCAN_BYTES = 1024 * 1024 * 3;
	private static final int HBASE_SCAN_LIMIT = 1;

	private void optimize(Scan s) {
		// optimize scan for performance, but hbase throw strang exception...
		try {
			s.setCaching(HBASE_SCAN_LIMIT);// rows per rpc
		} catch (Throwable t) {
			try {
				s.setCaching(HBASE_SCAN_LIMIT);// rows per rpc
			} catch (Throwable tt) {
				logger().debug("Hbase setCaching fail", tt);
			}
		}
		try {
			s.setBatch(HBASE_SCAN_LIMIT);// cols per rpc
		} catch (Throwable t) {
			try {
				s.setBatch(HBASE_SCAN_LIMIT);// cols per rpc
			} catch (Throwable tt) {
				logger().debug("Hbase setBatch fail", tt);
			}
		}
		try {
			s.setMaxResultSize(HBASE_SCAN_BYTES);
		} catch (Throwable t) {
			try {
				s.setMaxResultSize(HBASE_SCAN_BYTES);
			} catch (Throwable tt) {
				logger().debug("Hbase setMaxResultSize fail", tt);
			}
		}
	}
}
