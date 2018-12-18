package net.butfly.albatis.io;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Loggable;
import net.butfly.albatis.ddl.TableDesc;

public interface IOFactory extends Loggable {
	URISpec uri();

	@Deprecated
	<M extends Rmap> Input<M> createInput(TableDesc... table) throws IOException;

	default <M extends Rmap> Output<M> output(Map<String, String> keyMapping) throws IOException {
		List<TableDesc> l = Colls.list(keyMapping.entrySet(), e -> {
			TableDesc t = TableDesc.dummy(e.getKey());
			t.keys.add(Colls.list(e.getValue()));
			return t;
		});
		return output(l.toArray(new TableDesc[0]));
	}

	default <M extends Rmap> Output<M> output(String... table) throws IOException {
		return output(Colls.list(n -> TableDesc.dummy(n), table).toArray(new TableDesc[0]));
	}

	<M extends Rmap> Output<M> output(TableDesc... table) throws IOException;

	default Input<Rmap> input(String... table) throws IOException {
		return input(Colls.list(n -> TableDesc.dummy(n), table).toArray(new TableDesc[0]));
	}

	default Input<Rmap> input(TableDesc... table) throws IOException {
		Input<Rmap> i = createInput(table);
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

	default Input<Rmap> input(Map<String, String> keyMapping) throws IOException {
		List<TableDesc> l = Colls.list(keyMapping.entrySet(), e -> {
			TableDesc t = TableDesc.dummy(e.getKey());
			t.keys.add(Colls.list(e.getValue()));
			return t;
		});
		return input(l.toArray(new TableDesc[0]));
	}
}
