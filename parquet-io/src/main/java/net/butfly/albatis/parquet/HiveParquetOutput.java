package net.butfly.albatis.parquet;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.hadoop.fs.Path;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.parquet.impl.HashingStrategy;
import net.butfly.albatis.parquet.impl.HiveParquetWriter;
import net.butfly.albatis.parquet.impl.HiveParquetWriterHDFS;
import net.butfly.albatis.parquet.impl.HiveParquetWriterLocal;

public class HiveParquetOutput extends OutputBase<Rmap> {
	private static final long serialVersionUID = 4543231903669455241L;

	final HiveConnection conn;
	private Map<Path, HiveParquetWriter> writers = Maps.of();
	private final Thread monitor;

	public HiveParquetOutput(String name, HiveConnection conn, TableDesc... table) throws IOException {
		super(name);
		this.conn = conn;
		for (TableDesc t : table) {
			HashingStrategy s = HashingStrategy.get(t.attr(HiveParquetWriter.HASHING_STRATEGY_DESC_PARAM));
			if (null != s) t.attw(HiveParquetWriter.HASHING_STRATEGY_IMPL_PARAM, s);
			// w(t);// rolling initialization tables.
		}
		monitor = new Thread(() -> clear(), "HiveParquetFileWriterMonitor");
		monitor.setDaemon(true);
	}

	private HiveParquetWriter w(Qualifier table, String hashing) {
		Path path = new Path(new Path(conn.base, table.name), hashing);
		TableDesc td = schema(table);
		return writers.computeIfAbsent(path, p -> null == conn.conf ? //
				new HiveParquetWriterLocal(td, conn.conf, p, logger()) : //
				new HiveParquetWriterHDFS(td, conn.conf, p, logger()));
	}

	@Override
	protected void enqsafe(Sdream<Rmap> items) {
		Map<Qualifier, Map<String, List<Rmap>>> split = Maps.of();
		items.eachs(r -> split.computeIfAbsent(r.table(), t -> Maps.of())//
				.computeIfAbsent(hashing(r, schema(r.table())), h -> Colls.list()).add(r));
		split.forEach((t, m) -> m.forEach((h, l) -> w(t, h).write(l)));
	}

	private String hashing(Rmap r, TableDesc td) {
		HashingStrategy s = td.attr(HiveParquetWriter.HASHING_STRATEGY_IMPL_PARAM);
		if (null != s) {
			String hashingField = td.attr(HiveParquetWriter.HASHING_FIELD_NAME_PARAM);
			if (null != hashingField) {
				Object value = r.get(hashingField);
				if (null != value) return s.hashing(value);
			}
		}
		return "";
	}

	@Override
	public void close() {
		super.close();
		Path p;
		HiveParquetWriter w;
		while (true) {
			try {
				p = writers.keySet().iterator().next();
			} catch (NoSuchElementException e) {
				return;
			} ;
			if (null != (w = writers.remove(p))) w.close();
		}
	}

	private void clear() {
		long now = System.currentTimeMillis();
		while (true) {
			writers.forEach((p, w) -> {
				if (w.timeout > now - w.lastWriten.get())
					w.rolling();

			});
		}
	}
}
