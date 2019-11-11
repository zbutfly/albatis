package net.butfly.albatis.parquet;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public class HiveParquetOutput extends OutputBase<Rmap> {
	private static final long serialVersionUID = 4543231903669455241L;

	final HiveConnection conn;
	private Map<Qualifier, HiveParquetWriter> writers = Maps.of();

	public HiveParquetOutput(String name, HiveConnection conn, TableDesc... table) throws IOException {
		super(name);
		this.conn = conn;
		for (TableDesc t : table) rolling(t.qualifier);
	}

	private void rolling(Qualifier table) throws IOException {
		writers.compute(table, (t, w) -> null == w ? new HiveParquetWriter(this, t) : w.rolling());
	}

	@Override
	protected void enqsafe(Sdream<Rmap> items) {
		Map<Qualifier, List<Rmap>> split = Maps.of();
		items.eachs(r -> split.computeIfAbsent(r.table(), t -> Colls.list()));
		split.forEach((t, l) -> writers.computeIfAbsent(t, tt -> new HiveParquetWriter(this, tt)).write(l));
	}
}