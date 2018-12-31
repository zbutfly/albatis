package net.butfly.albatis.io;

import static net.butfly.albatis.io.format.Format.CONST_FORMAT;
import static net.butfly.albatis.io.format.Format.as;
import static net.butfly.albatis.io.format.Format.asOf;
import static net.butfly.albatis.io.format.Format.of;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.format.Format;
import net.butfly.alserder.SerDes;
import net.butfly.alserder.SerDes.As;

public interface Formatable extends Loggable {
	// core implementations
	default <M extends Rmap, I extends Input<M>> I inputRaw(TableDesc... table) throws IOException {
		throw new UnsupportedOperationException();
	}

	default <M extends Rmap, O extends Output<M>> O outputRaw(TableDesc... table) throws IOException {
		throw new UnsupportedOperationException();
	}

	URISpec uri();

	static final Map<Formatable, List<Pair<Format, SerDes.As>>> FORMAT_INSTANCES = Maps.of();

	default List<Pair<Format, SerDes.As>> formats() {
		return FORMAT_INSTANCES.computeIfAbsent(this, c -> {
			String format = uri().fetchParameter("df");
			List<Pair<Format, SerDes.As>> fmts = null == format ? Colls.list()
					: Colls.list(f -> new Pair<>(of(f), asOf(f)), format.split(","));

			As[] defAs = as(getClass());

			Pair<Format, SerDes.As> def = defAs.length == 1 ? new Pair<>(of(defAs[0].value()), asOf(defAs[0].value())) : null;
			if (def.v1().equals(CONST_FORMAT)) def = null;
			Pair<Format, SerDes.As> fmt1 = null == fmts || fmts.isEmpty() ? null : fmts.get(0);
			if (null == fmt1) {
				if (null == def) return Colls.list();
				logger().info("Non-format defined, default format [" + def.v2().value() + "] used.");
				return Colls.list(def);
			}
			if (null != defAs) {
				logger().info("Default format [" + def.v2().value() + "] is ignored by [" //
						+ String.join(",", Colls.list(fmts, f -> f.v2().value())) + "]");
			}
			return fmts;
		});

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	default <M extends Rmap> Input<M> input(List<TableDesc> tables, List<Pair<Format, SerDes.As>> fotmats) throws IOException {
		Input i = inputRaw(tables.toArray(new TableDesc[0]));
		// deserializing
		if (null != fotmats) for (Pair<Format, SerDes.As> f : fotmats)
			i = f.v2().list() //
					? i.thenFlat(r -> null == r || ((Rmap) r).isEmpty() ? Sdream.of() : Sdream.of((List<Rmap>) f.v1().desers((Rmap) r)))
					: i.then(r -> null == r || ((Rmap) r).isEmpty() ? Sdream.of() : Sdream.of((List<Rmap>) f.v1().deser((Rmap) r)));

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
	default <M extends Rmap> Output<M> output(List<TableDesc> tables, List<Pair<Format, SerDes.As>> fotmats) throws IOException {
		Output o = outputRaw(tables.toArray(new TableDesc[0]));
		// serializing
		if (null != fotmats) for (Pair<Format, SerDes.As> f : fotmats)
			o = f.v2().list() //
					? o.prior(r -> null == r || ((Rmap) r).isEmpty() ? null : f.v1().sers(Colls.list((Rmap) r)))
					: o.prior(r -> null == r || ((Rmap) r).isEmpty() ? null : f.v1().ser((Rmap) r));
		return o;
	}
}
