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
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.io.Rmap;

class HbaseTableScaner {
	final HbaseInput input;
	final String table;
	final ResultScanner scaner;
	final List<String> families = list();
	final List<String> prefixes = list();

	// public HbaseTableScaner(HbaseInput input, String table, Filter f, byte[]... startAndEndRow) throws IOException {
	// this(input, table, f, list(), list(), startAndEndRow);
	// }

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

	public Map<Qualifier, Rmap> next() {
		try {
			return result(scaner.next());
		} catch (IOException e) {
			return Maps.of();
		}
	}

	public boolean empty() {
		return !scaner.iterator().hasNext();
	}

	public Map<Qualifier, Rmap> result(Result r) {
		Map<Qualifier, Rmap> rmaps = Maps.of();
		String rowkey = Bytes.toString(r.getRow());
		for (Entry<String, byte[]> e : Hbases.map(r.listCells().stream()).entrySet()) {
			Pair<Qualifier, String> fq = Qualifier.qf(table).colkey(e.getKey());
			rmaps.computeIfAbsent(fq.v1(), q -> new Rmap(fq.v1(), rowkey)).put(fq.v2(), e.getValue());
		}
		return rmaps;
	}
}
