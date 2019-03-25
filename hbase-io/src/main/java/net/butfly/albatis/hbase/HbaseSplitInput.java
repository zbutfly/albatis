package net.butfly.albatis.hbase;

import static net.butfly.albatis.hbase.utils.HbaseScan.Range.range;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.hbase.filter.Filter;

import net.butfly.albacore.utils.collection.Colls;

public class HbaseSplitInput extends HbaseInput {
	private static final long serialVersionUID = 6225222417568739808L;

	public HbaseSplitInput(String name, HbaseConnection conn) {
		super(name, conn);
	}

	private TableScaner[] regions(String table, Collection<String> families, Collection<String> prefixes, Filter f,
			byte[]... startAndStopRow) throws IOException {
		return Colls.list(r -> TableScaner.of(this, table, families, prefixes, f, r), //
				range(startAndStopRow).restrict(hconn.ranges(table))).toArray(new TableScaner[0]);
	}

	@Override
	public void table(String table, Collection<String> families, Collection<String> prefixes, byte[]... startAndStopRow) throws IOException {
		regions(table, families, prefixes, null, startAndStopRow);
	}

	@Override
	public void table(String table) throws IOException {
		regions(table, null, null, null);
	}
}
