package net.butfly.albatis.io;

import static net.butfly.albacore.utils.collection.Colls.empty;
import static net.butfly.albatis.ddl.TableDesc.dummy;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.QualifierField;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.alserdes.format.Format;
import net.butfly.alserdes.format.Formatable;

public interface IOFactory extends Formatable {
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
	default <M extends Rmap> Input<M> input(List<TableDesc> tables, List<? extends Format> formats) throws IOException {
		Map<Qualifier, TableDesc> tbls = Maps.distinct(tables, t -> t.qualifier);
		Input i = inputRaw(tables.toArray(new TableDesc[0]));
		// subtable
		i = i.thenFlat(r -> subtable((Rmap) r, tbls));
		// deserializing
		if (!empty(formats)) {
			logger().info("Input [" + i.toString() + "] with formats: " + String.join(",", Colls.list(formats, f -> f.as().value())));
			for (Format f : formats) i = f.as().list() ? //
					i.thenFlat(r -> {
						if (empty((Rmap) r)) return Sdream.of();
						else return Sdream.of(f.desers((Rmap) r, tbls.get(((Rmap) r).table())));
					}) : i.then(r -> {
						if (empty((Rmap) r)) return null;
						else return f.deser((Rmap) r, tbls.get(((Rmap) r).table()));
					});
		}
		// key field filfulling
		Map<String, String> keys = Maps.of();
		for (TableDesc t : tables) if (null != t.rowkey()) keys.put(t.qualifier.name, t.rowkey());
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

	default Sdream<Rmap> subtable(Rmap m, Map<Qualifier, TableDesc> tbls) {
		if (empty(m)) return Sdream.of();
		Object rowkey = m.key();

		TableDesc td = tbls.get(m.table);
		if (null == td) {
			Map<Qualifier, Map<QualifierField, Object>> splitted = Maps.of(); // subtable -> colkey -> subdata
			m.forEach((f, v) -> {
				QualifierField fq = QualifierField.of(m.table, f);
				splitted.computeIfAbsent(fq.table, q -> Maps.of()).put(fq, v);// colkey -> subfield
			});

			Map<Qualifier, Map<String, Object>> subs = Maps.of();
			splitted.forEach((q, d) -> {
				Qualifier q1 = q;
				TableDesc td1 = tbls.get(q1);
				if (null == td1) td1 = tbls.get(q1 = q.family(null));
				if (null == td1) td1 = tbls.get(q1 = q.prefix(null));
				if (null == td1) td1 = tbls.get(q1 = q.family(null).prefix(null));
				if (null == td1) //
					throw new IllegalArgumentException("Rmap from [" + q + "] not found in registered table descs: " + tbls.toString());
				for (QualifierField colkey : d.keySet()) {
					QualifierField ff = colkey.table(q1);
					subs.computeIfAbsent(ff.table, tt -> Maps.of()).put(ff.name, d.remove(colkey));
				}
			});
			List<Rmap> lms = Colls.list(subs.entrySet(), e -> new Rmap(e.getKey(), rowkey, e.getValue()));
			return Sdream.of(lms);
		} else {
			for (String f : m.keySet()) {
				QualifierField fq = QualifierField.of(m.table, f).table(m.table);
				if (!fq.name.equals(f)) {
					m.put(fq.name, m.remove(f));
					if (f.equals(m.keyField)) m.keyField(fq.name);
				}
			}
			return Sdream.of1(m);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	default <M extends Rmap> Output<M> output(List<TableDesc> tables, List<? extends Format> fotmats) throws IOException {
		Map<Qualifier, TableDesc> tbls = Maps.distinct(tables, t -> t.qualifier);
		Output o = outputRaw(tables.toArray(new TableDesc[0]));
		// serializing
		if (null != fotmats) for (Format f : fotmats) o = f.as().list() //
				? o.prior(r -> empty((Rmap) r) ? null : f.sers(Colls.list((Rmap) r), (TableDesc) tbls.get(((Rmap) r).table())))
				: o.prior(r -> empty((Rmap) r) ? null : f.ser((Rmap) r, (TableDesc) tbls.get(((Rmap) r).table())));
		return o;
	}
}
