package net.butfly.albatis.io;

import static net.butfly.albatis.ddl.TableDesc.dummy;
import static net.butfly.albatis.io.Rmap.empty;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.format.RmapFormat;
import net.butfly.alserder.format.Formatable;

public interface IOFactory extends Formatable<Rmap, TableDesc, RmapFormat> {
	default <M extends Rmap> Input<M> input(TableDesc... table) throws IOException {
		return input(Arrays.asList(table), formats());
	}

	default <M extends Rmap> Input<M> input(String... table) throws IOException {
		return input(dummy(table), formats());
	}

	default <M extends Rmap> Input<M> input(Map<String, String> keyMapping) throws IOException {
		return input(dummy(keyMapping), formats());
	}

	default <M extends Rmap> Output<M> output() throws IOException {
		return output(Arrays.asList(), formats());
	}

	default <M extends Rmap> Output<M> output(TableDesc... table) throws IOException {
		return output(Arrays.asList(table), formats());
	}

	default <M extends Rmap> Output<M> output(String... table) throws IOException {
		return output(dummy(table), formats());
	}

	default <M extends Rmap> Output<M> output(Map<String, String> keyMapping) throws IOException {
		return output(dummy(keyMapping), formats());
	}

	// core implementations
	default <M extends Rmap, I extends Input<M>> I inputRaw(TableDesc... table) throws IOException {
		throw new UnsupportedOperationException();
	}

	default <M extends Rmap, O extends Output<M>> O outputRaw(TableDesc... table) throws IOException {
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	default <M extends Rmap> Input<M> input(List<TableDesc> tables, List<RmapFormat> fotmats) throws IOException {
		Map<String, TableDesc> tbls = Maps.distinct(tables, t -> t.name);
		Input i = inputRaw(tables.toArray(new TableDesc[0]));
		// deserializing
		if (null != fotmats) for (RmapFormat f : fotmats)
			i = f.as().list() //
					? i.thenFlat(r -> empty((Rmap) r) ? Sdream.of() : Sdream.of(f.disassembles((Rmap) r, tbls.get(((Rmap) r).table()))))
					: i.then(r -> empty((Rmap) r) ? null : f.disassemble((Rmap) r, tbls.get(((Rmap) r).table())));
		// key field filfulling
		Map<String, String> keys = Maps.of();
		for (TableDesc t : tables)
			if (null != t.rowkey()) keys.put(t.name, t.rowkey());
		if (!keys.isEmpty()) {
			logger().info("Key fields found, Input will fill the key field value: \n\t" + keys.toString());
			i = i.then(r -> {
				Rmap m = (Rmap) r;
				if (null != m.table()) {
					String kf = keys.getOrDefault(m.table(), keys.get("*"));
					if (null != kf) m.keyField(kf);
				}
				return m;
			});
		}
		return i;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	default <M extends Rmap> Output<M> output(List<TableDesc> tables, List<RmapFormat> fotmats) throws IOException {
		Map<String, TableDesc> tbls = Maps.distinct(tables, t -> t.name);
		Output o = outputRaw(tables.toArray(new TableDesc[0]));
		// serializing
		if (null != fotmats) for (RmapFormat f : fotmats)
			o = f.as().list() //
					? o.prior(r -> empty((Rmap) r) ? null : f.assembles(Colls.list((Rmap) r), tbls.get(((Rmap) r).table())))
					: o.prior(r -> empty((Rmap) r) ? null : f.assemble((Rmap) r, tbls.get(((Rmap) r).table())));
		return o;
	}
}
