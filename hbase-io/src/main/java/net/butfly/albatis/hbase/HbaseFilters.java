package net.butfly.albatis.hbase;

import static net.butfly.albatis.ddl.FieldDesc.SPLIT_PREFIX;

import java.util.List;

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
		return l.size() == 1 ? l.get(0) : new FilterList(l);
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
		if (1 == prefixes.size()) return null == prefixes.get(0) ? null : new ColumnPrefixFilter(Bytes.toBytes(prefixes.get(0) + SPLIT_PREFIX));
		byte[][] ps = Colls.list(prefixes, p -> Bytes.toBytes(p + SPLIT_PREFIX)).toArray(new byte[0][]);
		if (null == ps || ps.length == 0) return null;
		if (ps.length == 1) return new ColumnPrefixFilter(ps[0]);
		else return new MultipleColumnPrefixFilter(ps);
	}
}
