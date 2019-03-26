package net.butfly.albatis.hbase.utils;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.lambda.ConsumerPair;

public interface HbaseScan {
	final byte[] ROWKEY_UNDEFINED = new byte[0];

	class Options implements Serializable {
		private static final long serialVersionUID = -6899642819338598242L;
		public final int rowsRpc, colsRpc;
		public final long bytesRpc;
		public final boolean chachBlocks;

		public static Options opts(int rowsRpc, int colsRpc, long bytesRpc, boolean catchBlocks) {
			return new Options(rowsRpc, colsRpc, bytesRpc, catchBlocks);
		}

		private Options(int rowsRpc, int colsRpc, long bytesRpc, boolean catchBlocks) {
			super();
			this.rowsRpc = rowsRpc;
			this.colsRpc = colsRpc;
			this.bytesRpc = bytesRpc;
			this.chachBlocks = catchBlocks;
		}

		private <V> void set(Scan s, V v, ConsumerPair<Scan, V> set, Method fail) {
			try {
				set.accept(s, v);
			} catch (Throwable t) {
				try {
					fail.invoke(s, v);
				} catch (Throwable tt) {
					Hbases.logger.warn("Optimize fail on " + fail.toString(), tt);
				}
			}
		}

		public Scan optimize(Scan s) {
			if (rowsRpc > 0) set(s, rowsRpc, Scan::setCaching, SET_CHACHING);
			if (colsRpc > 0) set(s, colsRpc, Scan::setBatch, SET_BATCH);
			if (bytesRpc > 0) set(s, bytesRpc, Scan::setMaxResultSize, SET_MAX_RESULT_SIZE);
			set(s, chachBlocks, Scan::setCacheBlocks, SET_CACHE_BLOCKS);
			return s;
		}

		private static final Method SET_CHACHING, SET_BATCH, SET_MAX_RESULT_SIZE, SET_CACHE_BLOCKS;

		static {
			try {
				SET_CHACHING = Scan.class.getMethod("setCaching", int.class);
				SET_BATCH = Scan.class.getMethod("setBatch", int.class);
				SET_MAX_RESULT_SIZE = Scan.class.getMethod("setMaxResultSize", long.class);
				SET_CACHE_BLOCKS = Scan.class.getMethod("setCacheBlocks", boolean.class);
			} catch (NoSuchMethodException | SecurityException e) {
				throw new RuntimeException(e);
			}
		}
	}

	class Range implements Serializable {
		private static final long serialVersionUID = 8600945485268125960L;
		public final byte[] start, stop;
		private final boolean stopped, started;

		/**
		 * @return an unranged {@code Range}
		 */
		public static Range range() {
			return new Range((byte[][]) null);
		}

		/**
		 * @return validate the param {@code startAndStopRow} and construct
		 */
		public static Range range(byte[]... startAndStopRow) {
			Range range = new Range(startAndStopRow);
			if ((range.started ^ range.stopped) || //
					(range.started && range.stopped && lt(range.start, range.stop))) return range;
			return range();
		}

		private Range(byte[]... startAndStopRow) {
			super();
			if (null == startAndStopRow || startAndStopRow.length == 0) {
				start = stop = ROWKEY_UNDEFINED;
				started = stopped = false;
			} else {
				started = defined(startAndStopRow[0]);
				start = started ? startAndStopRow[0] : ROWKEY_UNDEFINED;
				stopped = startAndStopRow.length > 1 && defined(startAndStopRow[1]);
				stop = stopped ? startAndStopRow[1] : ROWKEY_UNDEFINED;
			}
		}

		public boolean include(byte[] row) {
			if (!defined(row)) return !(started && stopped);
			if (started && stopped) return !lt(row, start) && lt(row, stop);
			if (started) return !lt(row, start); // gte
			if (stopped) return lt(row, stop);
			return true;
		}

		public boolean ranged() {
			return started || stopped;
		}

		public Range[] restrict(Range... origin) {
			if (!ranged()) return origin;
			List<Range> restricted = new ArrayList<>();
			Range r;
			for (Range range : origin)
				if (null != (r = range(max(start, range.start), min(stop, range.stop)))) restricted.add(r);
			return restricted.toArray(new Range[0]);
		}

		private static boolean defined(byte[] rowkey) {
			return null != rowkey && rowkey.length > 0;
		}

		private static boolean lt(byte[] r0, byte[] r1) {
			return new BinaryComparator(r0).compareTo(r1) < 0;
		}

		private byte[] min(byte[] r0, byte[] r1) {
			return !defined(r0) || !defined(r1) ? ROWKEY_UNDEFINED : lt(r0, r1) ? r0 : r1;
		}

		private byte[] max(byte[] r0, byte[] r1) {
			return !defined(r0) || !defined(r1) ? ROWKEY_UNDEFINED : lt(r0, r1) ? r1 : r0;
		}

		public Scan scan() {
			return scan(null);
		}

		public Scan scan(Filter f) {
			Scan s = new Scan();
			if (defined(start)) s.setStartRow(start);
			if (defined(stop)) s.setStopRow(stop);
			if (null != f) s.setFilter(f);
			return s;
		}

		@Override
		public String toString() {
			return "[" + (defined(start) ? Bytes.toString(start) : "") + " ~ " + (defined(stop) ? Bytes.toString(stop) : "") + ")";
		}
	}
}
