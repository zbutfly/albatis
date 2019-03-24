package net.butfly.albatis.hbase;

import java.util.List;

import org.apache.hadoop.hbase.filter.Filter;

import net.butfly.albacore.utils.collection.Colls;

public class HbaseSplitInput extends HbaseInput {
	private static final long serialVersionUID = 6225222417568739808L;

	public HbaseSplitInput(String name, HbaseConnection conn) {
		super(name, conn);
	}

	private List<HbaseTableScaner> regions(String table, String logicalTable, Filter f, byte[]... startAndEndRow) {
		if (null == startAndEndRow || 0 == startAndEndRow.length) // split into regions
			return Colls.list(hconn.ranges(table), range -> new HbaseTableScaner(this, table, logicalTable, f, range.v1(), range.v2()));
		else if (1 == startAndEndRow.length) return Colls.list(new HbaseTableScaner(this, table, logicalTable, f, startAndEndRow[0]));
		else return Colls.list(new HbaseTableScaner(this, table, logicalTable, f, startAndEndRow[0], startAndEndRow[1]));
	}

	@Override
	protected void table(String table, String logicalTable, Filter filter) {
		regions(table, logicalTable, filter);
	}

	@Override
	public void table(String table, String logicalTable, byte[]... startAndEndRow) {
		regions(table, logicalTable, null, startAndEndRow);
	}

	@Override
	public void table(String table, String logicalTable) {
		regions(table, logicalTable, null);
	}
}
