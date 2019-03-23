package net.butfly.albatis.hbase;

import static net.butfly.albatis.ddl.FieldDesc.SPLIT_PREFIX;
import static net.butfly.albatis.hbase.HbaseInput.SCAN_BYTES;
import static net.butfly.albatis.hbase.HbaseInput.SCAN_CACHE_BLOCKS;

import java.lang.reflect.Method;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.utils.collection.Colls;

public interface HbaseFilters {
	static Filter limitCells(Filter f) {
		return and(f, new ColumnCountGetFilter(HbaseInput.SCAN_MAX_CELLS_PER_ROW));
	}

	static Filter or(Filter... fl) {
		List<Filter> l = Colls.list();
		for (Filter f : fl)
			if (null != f) l.add(f);
		if (l.isEmpty()) return null;
		return l.size() == 1 ? l.get(0) : new FilterList(Operator.MUST_PASS_ONE, l);
	}

	static Filter and(Filter... fl) {
		List<Filter> l = Colls.list();
		for (Filter f : fl)
			if (null != f) l.add(f);
		if (l.isEmpty()) return null;
		return l.size() == 1 ? l.get(0) : new FilterList(Operator.MUST_PASS_ALL, l);
	}

	static Filter filterFamily(String... cf) {
		if (null == cf || 0 == cf.length) return null;
		List<Filter> fl = Colls.list();
		for (String c : cf)
			fl.add(new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(c))));
		return fl.size() == 1 ? fl.get(0) : new FilterList(Operator.MUST_PASS_ONE, fl);
	}

	static Filter filterPrefix(List<String> prefixes) {
		if (Colls.empty(prefixes)) return null;
		if (1 == prefixes.size()) return null == prefixes.get(0) ? null
				: new ColumnPrefixFilter(Bytes.toBytes(prefixes.get(0) + SPLIT_PREFIX));
		byte[][] ps = Colls.list(prefixes, p -> Bytes.toBytes(p + SPLIT_PREFIX)).toArray(new byte[0][]);
		if (null == ps || ps.length == 0) return null;
		if (ps.length == 1) return new ColumnPrefixFilter(ps[0]);
		else return new MultipleColumnPrefixFilter(ps);
	}


	public static Scan optimize(Scan s, int rowsPerRpc, int colsPerRpc) {
		// optimize scan for performance, but hbase throw strang exception...
		if (rowsPerRpc > 0) try {
			s.setCaching(rowsPerRpc);// rows per rpc
		} catch (Throwable t) {
			try {
				$priv$.SET_CHACHING.invoke(s, rowsPerRpc);
			} catch (Throwable tt) {
				Hbases.logger.warn("Optimize scan Caching fail", tt);
			}
		}
		if (colsPerRpc > 0) try {
			s.setBatch(colsPerRpc);// cols per rpc
		} catch (Throwable t) {
			try {
				$priv$.SET_BATCH.invoke(s, colsPerRpc);// cols per rpc
			} catch (Throwable tt) {
				Hbases.logger.warn("Optimize scan Batch fail", tt);
			}
		}
		if (SCAN_BYTES > 0) try {
			s.setMaxResultSize(SCAN_BYTES);
		} catch (Throwable t) {
			try {
				$priv$.SET_MAX_RESULT_SIZE.invoke(s, SCAN_BYTES);
			} catch (Throwable tt) {
				Hbases.logger.warn("Optimize scan MaxResultSize fail", tt);
			}
		}
		try {
			s.setCacheBlocks(SCAN_CACHE_BLOCKS);
		} catch (Throwable t) {
			try {
				$priv$.SET_CACHE_BLOCKS.invoke(s, SCAN_CACHE_BLOCKS);
			} catch (Throwable tt) {
				Hbases.logger.warn("Optimize scan MaxResultSize fail", tt);
			}
		}
		return s;
	}

	static class $priv$ {
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
}
