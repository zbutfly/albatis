package net.butfly.albatis.arangodb;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import com.arangodb.ArangoCursorAsync;
import com.arangodb.entity.BaseDocument;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.OutputBase;

public class ArangoOutput extends OutputBase<EdgeMessage> {
	private final ArangoConnection conn;

	protected ArangoOutput(String name, ArangoConnection conn) {
		super(name);
		this.conn = conn;
	}

	@Override
	protected void enqueue0(Sdream<EdgeMessage> edges) {
		List<CompletableFuture<ArangoCursorAsync<BaseDocument>>> l = edges.map(e -> {
			Message v;
			CompletableFuture<ArangoCursorAsync<BaseDocument>> f = null;
			if (null != (v = e.start())) f = async(v);
			if (null != (v = e.end())) f = null == f ? async(v) : f.thenCombine(async(v), (c1, c2) -> null);
			return f.thenCompose(n -> {
				AtomicReference<CompletableFuture<ArangoCursorAsync<BaseDocument>>> ref = new AtomicReference<>(async(e));
				e.edges.forEach(ee -> ref.set(ref.get().thenCombine(async(ee), (c1, c2) -> null)));
				return ref.get();
			});
		}).list();
		CompletableFuture.allOf(l.toArray(new CompletableFuture[l.size()]));
	}

	private static final String EDGE_AQL = "upsert data._key == @key insert @data update {} in @col";

	private CompletableFuture<ArangoCursorAsync<BaseDocument>> async(Message v) {
		CompletableFuture<ArangoCursorAsync<BaseDocument>> f = conn.client().db().query(EDGE_AQL, Maps.of("data", v.map(), "col", v.table(),
				"key", v.key()), null, BaseDocument.class);
		// ArangoCursorAsync<BaseDocument> cc = f.get();
		return f;
	}
}
