package net.butfly.albatis.hbase;

import static net.butfly.albacore.utils.collection.Colls.list;
import static net.butfly.albatis.hbase.HbaseInput.SCAN_BYTES;
import static net.butfly.albatis.hbase.HbaseInput.SCAN_CACHE_BLOCKS;
import static net.butfly.albatis.hbase.HbaseInput.SCAN_COLS;
import static net.butfly.albatis.hbase.Hbases.scan0;
import static net.butfly.albatis.hbase.Hbases.Filters.and;
import static net.butfly.albatis.hbase.Hbases.Filters.filterFamily;
import static net.butfly.albatis.hbase.Hbases.Filters.filterPrefix;
import static net.butfly.albatis.hbase.Hbases.ScanOption.opt;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.io.Rmap;

class HbaseTableScaner {
	final HbaseInput input;
	final String table;
	final ResultScanner scaner;
	final List<String> families = list();
	final List<String> prefixes = list();

	public HbaseTableScaner(HbaseInput input, String table, Collection<String> families, Collection<String> prefixes, Filter f,
			byte[]... startAndEndRow) throws IOException {
		super();
		input.SCAN_REGS.computeIfAbsent(table, t -> list()).add(this);
		input.SCAN_POOL.offer(this);
		this.input = input;
		this.table = table;
		if (null != families) this.families.addAll(families);
		if (null != prefixes) this.prefixes.addAll(prefixes);
		f = and(f, filterFamily(families), filterPrefix(prefixes));
		Scan sc = opt(input.batchSize(), SCAN_COLS, SCAN_BYTES, SCAN_CACHE_BLOCKS).optimize(scan0(f, startAndEndRow));
		scaner = input.hconn.table(table).getScanner(sc);
	}

	public void close() {
		try {
			scaner.close();
		} catch (Exception e) {} finally {
			input.SCAN_REGS.remove(table);
		}
	}

	public boolean empty() {
		return !scaner.iterator().hasNext();
	}

	/**
	 * @return Rmap with physical table name and qualified field name
	 */
	public Rmap next() {
		Result r;
		try {
			r = scaner.next();
		} catch (IOException e) {
			return null;
		}
		return null == r ? null : new Rmap(Qualifier.qf(table, null, null), Bytes.toString(r.getRow()), Hbases.Results.values(r));
	}

	private Rmap last = null;

	public Rmap fetchLast() {
		Rmap l = last;
		last = null;
		return l;
	}

	private static void merge(Map<String, Rmap> wholes, Rmap... r) {
		for (Rmap m : r)
			if (!Colls.empty(m)) wholes.compute((String) m.key(), (q, existed) -> {
				if (null == existed) return m;
				existed.putAll(m);
				return existed;
			});
	}

	// return scan can be continue
	private boolean last(Map<String, Rmap> wholes) {
		Rmap next;
		if (null == (next = next())) {
			if (null != last) merge(wholes, last);
			return false;
		} else {
			Rmap l;
			if (null != (l = wholes.remove(next.key()))) next.putAll(l); // same record, move from wholes into next
			if (null != last) {
				if (!last.key().equals(next.key())) //
					HbaseInput.logger.error("Records row key conflicted on merge of last and next:"//
							+ "\n\tlast: " + last + "\n\tnext: " + next);
				last.putAll(next);
			} else last = next;
			return true;
		}
	}

	// return scan can be continue
	boolean dequeue(Map<String, Rmap> wholes) {
		Rmap orig, last;
		if (((null != (orig = next())))) return true;
		if (null != (last = fetchLast())) {
			Rmap m = wholes.get(last.key());
			if (null != m) m.putAll(last);
			else wholes.put(last.key().toString(), last);
		}
		merge(wholes, orig);
		if (last(wholes)) return true;
		merge(wholes, fetchLast());
		return false;
	}
}
