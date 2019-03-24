package net.butfly.albatis.hbase;

import static net.butfly.albatis.hbase.HbaseConnection.ROWKEY_UNDEFINED;
import static net.butfly.albatis.hbase.Hbases.scan0;
import static net.butfly.albatis.hbase.Hbases.Filters.and;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.logger.Logger;

class HbaseSkip {
	private static final Logger logger = Logger.getLogger(HbaseSkip.class);
	private final HbaseConnection conn;

	enum SkipMode {
		ROWS, REGIONS, REGION_COUNT, ROWKEY;
		public static Pair<SkipMode, String> parse(String opt) {
			if (null == opt) return null;
			for (SkipMode m : SkipMode.values()) {
				String prefix = m.name() + ":";
				if (opt.startsWith(prefix)) return new Pair<>(m, opt.substring(prefix.length()));
			}
			return new Pair<>(ROWKEY, opt);
		}
	}

	public HbaseSkip(HbaseConnection conn) {
		super();
		this.conn = conn;
	}

	public long count(String table, byte[] start, byte[] stop, String info) throws IOException {
		try (AggregationClient ac = new AggregationClient(conn.client.getConfiguration());) {
			// "region [1~" + SCAN_SKIP_EST_REGIONS + "]"
			info = "\n\t" + info + " rows [" + Bytes.toString(start) + "] ~ [" + Bytes.toString(stop) + "].";
			long now = System.currentTimeMillis();
			logger.warn("Hbase table [" + table + "] count: " + info);
			long step = -1;
			try {
				return step = ac.rowCount(TableName.valueOf(table), new LongColumnInterpreter(), scan0(start, stop));
			} finally {
				logger.warn("Hbase table [" + table + "] count [" + (step >= 0 ? step : "fail") //
						+ "] in [" + (System.currentTimeMillis() - now) / 10 / 100.0 + " seconds]:  by " + info);
			}
		} catch (Throwable e) {
			if (e instanceof IOException) throw (IOException) e;
			if (e instanceof RuntimeException) throw (RuntimeException) e;
			throw new IOException(e);
		}
	}

	public byte[] skipByRegionNum(String table, int regions, Filter f) throws IOException {
		if (regions <= 0) return ROWKEY_UNDEFINED;
		logger.warn("Hbase table [" + table + "] skip [" + regions + "] regions.");
		List<Pair<byte[], byte[]>> ranges = conn.ranges(table);
		if (regions >= ranges.size()) return ROWKEY_UNDEFINED;
		return ranges.get(regions - 1).v2();
	}

	public byte[] skipByRegionCount(String table, long skip, Filter f) throws IOException {
		if (skip <= 0) return ROWKEY_UNDEFINED;
		long c = 0, step = 0, i = 0;
		byte[] last = ROWKEY_UNDEFINED;
		for (Pair<byte[], byte[]> range : conn.ranges(table)) {
			last = range.v2();
			if (skip <= 0) break;
			step = count(table, range.v1(), range.v2(), "region [" + (++i) + "]");
			if (c + step > skip) break;
			else c += step;
		}
		if (skip > 0) last = skipByScan(table, skip, f, last);
		logger.warn("Hbase scan on table [" + table + "] finished, scan start from rowkey [" + Bytes.toString(last) + "].");
		return last;
	}

	public byte[] skipByScan(String table, long skip, Filter f, byte[]... startAndStopRow) throws IOException {
		Result r = null;
		long bytes = 0, cells = 0, rpcms = 0;
		Scan s = scan0(and(f, new FirstKeyOnlyFilter(), new KeyOnlyFilter()), startAndStopRow);
		try (ResultScanner rs = conn.table(table).getScanner(s)) {
			for (long i = 0; i < skip; i++) {
				if (i % 50000 == 0) {
					logger.warn("Hbase scan on table [" + table + "] skipping... now [" + i + "], "//
							+ "current rowkey [" + (null == r ? null : Bytes.toString(r.getRow())) + "], "//
							+ "skiped bytes/cells [" + bytes + "/" + cells + "], rpc 50000 time [" + ((rpcms / 10) / 100.0) + " secs].");
					rpcms = 0;
				}
				long now = System.currentTimeMillis();
				if (null == (r = rs.next())) throw new IllegalArgumentException("Hbase table [" + table + "] skipping scaned [" + i
						+ "] and finished, maybe caused by start/end row if set.");
				else {
					rpcms += (System.currentTimeMillis() - now);
					bytes += Hbases.totalCellSize(r);
					cells += r.size();
				}
			}
		}
		byte[] row = r.getRow();
		logger.warn("Hbase scan on table [" + table + "] skip [" + skip + "], "//
				+ "really scan start from: [" + Bytes.toString(row) + "].");
		return row;
	}
}
