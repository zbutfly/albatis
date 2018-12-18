package net.butfly.albatis.io;

import static net.butfly.albatis.ddl.TableDesc.dummy;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.alserder.SD;

public interface IOFactory extends Loggable {
	URISpec uri();

	// implementations
	<M extends Rmap> Input<M> createInput(TableDesc... table) throws IOException;

	<M extends Rmap> Output<M> createOutput(TableDesc... table) throws IOException;

	@SuppressWarnings("rawtypes")
	default List<SD> serders(String... formats) {
		return Colls.list(f -> f.isEmpty() ? null : SD.lookup(f), formats);
	}

	// apis
	@SuppressWarnings({ "rawtypes", "unchecked" })
	default <M extends Rmap> Input<M> input(TableDesc... table) throws IOException {
		Input<M> i = createInput(table);
		// deserializing
		String format = uri().fetchParameter("df");
		if (null != format) {
			List<SD> sds = serders(format.split(","));// ;
			for (SD sd : sds)
				i = i.then(m -> {
					for (String k : m.keySet())
						m.put(k, sd.der(m.get(k)));
					return m;
				});
		}
		// key field filfulling
		Map<String, String> keys = Maps.of();
		for (TableDesc t : table)
			if (null != t.rowkey()) keys.put(t.name, t.rowkey());
		if (!keys.isEmpty()) {
			logger().info("Key fields found, Input will fill the key field value: \n\t" + keys.toString());
			i = i.then(m -> {
				if (null != m.table()) {
					String kf = keys.get(m.table());
					if (null != kf) m.keyField(kf);
				}
				return m;
			});
		}
		return i;
	}

	@SuppressWarnings("unchecked")
	default <M extends Rmap> Output<M> output(TableDesc... table) throws IOException {
		Output<M> o = createOutput(table);
		// serializing
		String format = uri().fetchParameter("df");
		if (null != format) {
			for (String f : format.split(",")) {
				if (f.isEmpty()) continue;
				@SuppressWarnings("rawtypes")
				SD sd = SD.lookup(format);
				o = o.prior(m -> {
					for (String k : m.keySet())
						m.put(k, sd.ser(m.get(k)));
					return m;
				});
			}
		}
		return o;
	}

	// other format
	default <M extends Rmap> Input<M> input(String... table) throws IOException {
		return input(dummy(table));
	}

	default <M extends Rmap> Output<M> output(String... table) throws IOException {
		return output(dummy(table));
	}

	@Deprecated
	default <M extends Rmap> Output<M> output(Map<String, String> keyMapping) throws IOException {
		return output(dummy(keyMapping));
	}

	@Deprecated
	default Input<Rmap> input(Map<String, String> keyMapping) throws IOException {
		return input(dummy(keyMapping));
	}
}
