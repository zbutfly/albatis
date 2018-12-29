package net.butfly.albatis.io.format;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Rmap;

public class BsonFormat implements Format {
	private static final long serialVersionUID = -8348208179985245450L;

	@Override
	public Rmap ser(List<Rmap> l) {
		AtomicReference<String> table = new AtomicReference<String>();
		Map<String, byte[]> m = Maps.of();
		l.forEach(r -> {
			table.set(r.table());
			if (null != r.key()) m.compute((String) r.key(), (k, o) -> {
				if (null == o) return BsonSerder.map(r.map());
				logger().warn("Key conflicted: \n\texisted: " + o.toString() + "\n\tignored: " + r.toString());
				return o;
			});
		});
		return new Rmap(table.get());
	}

	@Override
	public List<Rmap> deser(Rmap r) {
		return Colls.list(r.keySet(), k -> new Rmap(r.table(), r.key(), BsonSerder.map((byte[]) r.get(k))));
	}
}
