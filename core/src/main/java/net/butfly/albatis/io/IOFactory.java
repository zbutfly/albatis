package net.butfly.albatis.io;

import static net.butfly.albatis.ddl.TableDesc.dummy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.alserder.SerDes;
import net.butfly.alserder.json.JsonsSerDes;

public interface IOFactory extends Loggable {
	// core implementations
	default <M extends Rmap> Input<M> inputRaw(TableDesc... table) throws IOException {
		throw new UnsupportedOperationException();
	}

	default <M extends Rmap> Output<M> outputRaw(TableDesc... table) throws IOException {
		throw new UnsupportedOperationException();
	}

	// core apis
	@SuppressWarnings({ "rawtypes", "unchecked" })
	default <M extends Rmap> Input<M> input(List<TableDesc> tables, List<SerDes> sds) throws IOException {
		Input<M> i = inputRaw(tables.toArray(new TableDesc[0]));
		// deserializing
		if (null != sds) for (SerDes sd : sds) {
			boolean bf = _Priv.isSerdesAsRmap(sd, true);
			i = i.thenFlat(m -> {
				if (null == m) return null;
				Object v;
				if (bf) return null == (v = sd.deser(m)) ? null : (v instanceof List ? Sdream.of((List<M>) v) : Sdream.of1((M) v));
				List<M> list = new ArrayList<>();
				for (String k : m.keySet()) {
					Object deser = sd.deser(m.get(k));
					if (sd instanceof JsonsSerDes) {
						((List<Map>) deser).forEach(p -> list.add((M) new Rmap(m.table(), m.key(), p)));
					} else m.put(k, deser);
				}
				if (!list.isEmpty()) return Sdream.of(list);
				return Sdream.of1(m);
			});
		}
		// key field filfulling
		Map<String, String> keys = Maps.of();
		for (TableDesc t : tables)
			if (null != t.rowkey()) keys.put(t.name, t.rowkey());
		if (!keys.isEmpty()) {
			logger().info("Key fields found, Input will fill the key field value: \n\t" + keys.toString());
			i = i.then(m -> {
				if (null != m.table()) {
					String kf = keys.getOrDefault(m.table(), keys.get("*"));
					if (null != kf) m.keyField(kf);
				}
				return m;
			});
		}
		return i;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	default <M extends Rmap> Output<M> output(List<TableDesc> tables, List<SerDes> sds) throws IOException {
		Output<M> o = outputRaw(tables.toArray(new TableDesc[0]));
		// serializing
		if (null != sds) for (SerDes sd : sds) {
			boolean bf = _Priv.isSerdesAsRmap(sd, false);
			o = o.priorFlat(m -> {
				if (null == m) return null;
				Object v;
				if (bf) return null == (v = sd.ser(m)) ? null : (v instanceof List ? Sdream.of((List<M>) v) : Sdream.of1((M) v));
				for (String k : m.keySet())
					m.put(k, sd.ser(m.get(k)));
				return Sdream.of1(m);
			});
		}
		return o;
	}

	// other format

	default <M extends Rmap> Input<M> input(TableDesc... table) throws IOException {
		return input(Arrays.asList(table), serdes());
	}

	default <M extends Rmap> Input<M> input(String... table) throws IOException {
		return input(dummy(table), serdes());
	}

	default Input<Rmap> input(Map<String, String> keyMapping) throws IOException {
		return input(dummy(keyMapping), serdes());
	}

	default <M extends Rmap> Output<M> output() throws IOException {
		return output(Arrays.asList(), serdes());
	}

	default <M extends Rmap> Output<M> output(TableDesc... table) throws IOException {
		return output(Arrays.asList(table), serdes());
	}

	default <M extends Rmap> Output<M> output(String... table) throws IOException {
		return output(dummy(table), serdes());
	}

	default <M extends Rmap> Output<M> output(Map<String, String> keyMapping) throws IOException {
		return output(dummy(keyMapping), serdes());
	}

	// utils

	URISpec uri();

	@SuppressWarnings({ "rawtypes", "unchecked" })
	default List<SerDes> serdes() {
		String format = uri().fetchParameter("df");
		String[] formats = null == format ? new String[0] : format.split(",");
		return null == format ? Colls.list() : Colls.list(SerDes::sd, formats);
	}

	class _Priv {
		/**
		 * on input, deser(), toClass is source class.<br>
		 * on output, ser(), fromClass is source class.<br>
		 * so the func return the <code>source class of serder is rmap</code>, means process whole record.<br>
		 * if not, processing fields each by each.<br>
		 * Maybe it can use Map, not Rmap
		 */
		@SuppressWarnings("rawtypes")
		private static boolean isSerdesAsRmap(SerDes sd, boolean input) {
			return Rmap.class.isAssignableFrom(input ? sd.toClass() : sd.fromClass());
		}
	}
}
