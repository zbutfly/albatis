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
import net.butfly.albatis.parquet.impl.HashingStrategy;
import net.butfly.albatis.parquet.impl.HiveParquetWriter;
import net.butfly.albatis.parquet.impl.HiveParquetWriterHDFS;
import net.butfly.albatis.parquet.impl.HiveParquetWriterLocal;

public class HiveParquetOutput extends OutputBase<Rmap> {
	private static final long serialVersionUID = 4543231903669455241L;

	final HiveConnection conn;
	private Map<Qualifier, HiveParquetWriter> writers = Maps.of();

	public HiveParquetOutput(String name, HiveConnection conn, TableDesc... table) throws IOException {
		super(name);
		this.conn = conn;
		for (TableDesc t : table) {
			HashingStrategy s = HashingStrategy.get(t.attr(HiveParquetWriter.HASHING_STRATEGY_DESC_PARAM, "DATE:yyyy/MM/dd"));
			if (null != s) t.attw(HiveParquetWriter.HASHING_STRATEGY_IMPL_PARAM, s);
			// w(t);// rolling initialization tables.
		}
	}

	private HiveParquetWriter w(Qualifier t, String hashing) {
		return writers.computeIfAbsent(t, tt -> null == conn.conf ? //
				new HiveParquetWriterLocal(this, tt, conn.conf, conn.base) : //
				new HiveParquetWriterHDFS(this, tt, conn.conf, conn.base));
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
}
