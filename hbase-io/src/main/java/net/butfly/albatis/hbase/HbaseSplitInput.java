package net.butfly.albatis.hbase;

import static net.butfly.albatis.hbase.Hbases.validRow;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;

import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Colls;

public class HbaseSplitInput extends HbaseInput {
	private static final long serialVersionUID = 6225222417568739808L;

	public HbaseSplitInput(String name, HbaseConnection conn) {
		super(name, conn);
	}

	private List<HbaseTableScaner> regions(String table, Collection<String> families, Collection<String> prefixes, Filter f,
			byte[]... startAndEndRow) throws IOException {
		List<HbaseTableScaner> l = Colls.list();
		for (Pair<byte[], byte[]> range : hconn.ranges(table)) {
			if (null != startAndEndRow && startAndEndRow.length > 0) { // range given, calc the intersection
				if (startAndEndRow.length > 1 && validRow(startAndEndRow[1]) // given stop, and...
						&& (!validRow(range.v2()) || // last reigion, or...
								new BinaryComparator(range.v2()).compareTo(startAndEndRow[1]) > 0 // range stop greater than given stop
						)) range.v2(startAndEndRow[1]);// use given stop
				if (validRow(startAndEndRow[0]) // given start, and...
						&& (!validRow(range.v1()) || // first reigion, or...
								new BinaryComparator(range.v1()).compareTo(startAndEndRow[0]) < 0 // range start less than given start
						)) range.v1(startAndEndRow[1]);// use given start
				// at last, if empty range, skip the region
				if (validRow(range.v1()) && validRow(range.v2()) && new BinaryComparator(range.v1()).compareTo(range.v2()) >= 0) continue;
			} // else: without range given, all region scaned
			l.add(new HbaseTableScaner(this, table, families, prefixes, f, range.v1(), range.v2()));
		}
		return l;
	}

	@Override
	public void table(String table, Collection<String> families, Collection<String> prefixes, byte[]... startAndEndRow) throws IOException {
		regions(table, families, prefixes, null, startAndEndRow);
	}

	@Override
	public void table(String table) throws IOException {
		regions(table, null, null, null);
	}
}
