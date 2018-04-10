package net.butfly.albatis.arangodb;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.arangodb.ArangoCursorAsync;
import com.arangodb.entity.BaseDocument;

import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.OutputBase;

public class ArangoOutput extends OutputBase<Message> {
	private final ArangoConnection conn;

	protected ArangoOutput(String name, ArangoConnection conn) {
		super(name);
		this.conn = conn;
	}

	@Override
	protected void enqueue0(Sdream<Message> items) {
		Map<String, List<Message>> m = Maps.of();
		List<Future<?>> fs = Colls.list();
		items.eachs(e -> {
			if (e instanceof EdgeMessage) //
				fs.add(writeEdgeWithVertexes((EdgeMessage) e));
			else m.compute(e.table(), (t, o) -> {
				if (null == o) o = Colls.list();
				o.add(e);
				return o;
			});
		});
		Sdream.of(m).eachs(e -> fs.add(conn.db.collection(e.getKey()).insertDocuments(e.getValue())));
		for (Future<?> f : fs)
			try {
				f.get();
			} catch (InterruptedException e) {} catch (ExecutionException e) {
				e.getCause().printStackTrace();;
			}
	}

	private static final String EDGE_AQL = "upsert data._key == @key insert @data update {} in @col";

	private Future<?> writeEdgeWithVertexes(EdgeMessage edge) {
		return Exeter.of().submit(() -> {
			CompletableFuture<ArangoCursorAsync<BaseDocument>> f0 = conn.client().db().query(EDGE_AQL, Maps.of("data", edge.vertexes[0]
					.map(), "col", edge.vertexes[0].table(), "key", edge.vertexes[0].key()), null, BaseDocument.class);
			CompletableFuture<ArangoCursorAsync<BaseDocument>> f1 = conn.client().db().query(EDGE_AQL, Maps.of("data", edge.vertexes[1]
					.map(), "col", edge.vertexes[1].table(), "key", edge.vertexes[1].key()), null, BaseDocument.class);
			f0.join();
			f1.join();
			conn.client().db().query(EDGE_AQL, Maps.of("data", edge.map(), "col", edge.table(), "key", edge.key()), null,
					BaseDocument.class).join();
		});
	}
}
