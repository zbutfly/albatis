package net.butfly.albatis.io;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.TableDesc;

public interface IOSupport {
	default <M extends Rmap> Input<M> input(String... table) throws IOException {
		return input(Colls.list(n -> TableDesc.dummy(n), table).toArray(new TableDesc[0]));
	}

	default <M extends Rmap> Input<M> input(Map<String, String> keyMapping) throws IOException {
		List<TableDesc> l = Colls.list(keyMapping.entrySet(), e -> {
			TableDesc t = TableDesc.dummy(e.getKey());
			t.keys.add(Colls.list(e.getValue()));
			return t;
		});
		return input(l.toArray(new TableDesc[0]));
	}

	<M extends Rmap> Input<M> input(TableDesc... table) throws IOException;

	default <M extends Rmap> Output<M> output(Map<String, String> keyMapping) throws IOException{
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
}
