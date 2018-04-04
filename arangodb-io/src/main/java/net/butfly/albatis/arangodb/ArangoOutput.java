package net.butfly.albatis.arangodb;

import java.util.List;
import java.util.Map;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.OutputBase;

public class ArangoOutput extends OutputBase<EdgeMessage> {
	private final ArangoConnection conn;

	protected ArangoOutput(String name, ArangoConnection conn) {
		super(name);
		this.conn = conn;
	}

	@Override
	protected void enqueue0(Sdream<EdgeMessage> items) {
		Map<String, List<EdgeMessage>> m = Maps.of();
		items.eachs(e -> {
			m.compute(e.table(), (t, o) -> {
				if (null == o) o = Colls.list();
				o.add(e);
				return o;
			});
		});
		m.forEach((t, ms) -> conn.db.collection(t).insertDocuments(ms));
	}
}
