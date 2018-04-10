package net.butfly.albatis.arangodb;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import com.arangodb.ArangoCursorAsync;
import com.arangodb.entity.BaseDocument;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
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
	public void close() {
		try {
			conn.close();
		} catch (IOException e) {
			throw new RuntimeException();
		}
	}

	private ArangoCursorAsync<BaseDocument> arangoStats(ArangoCursorAsync<BaseDocument> c1, ArangoCursorAsync<BaseDocument> c2) {
		return null;
	}

	@Override
	protected void enqueue0(Sdream<EdgeMessage> edges) {
		List<Map<String, Object>> statsList = Colls.list();
		List<CompletableFuture<ArangoCursorAsync<BaseDocument>>> l = edges.map(e -> {
			Message v;
			CompletableFuture<ArangoCursorAsync<BaseDocument>> f = null;
			if (null != (v = e.start())) f = async(v, statsList);
			if (null != (v = e.end())) f = null == f ? async(v, statsList) : f.thenCombine(async(v, statsList), this::arangoStats);
			return f.thenCompose(n -> {
				AtomicReference<CompletableFuture<ArangoCursorAsync<BaseDocument>>> ref = new AtomicReference<>(async(e, statsList));
				e.edges.forEach(ee -> ref.set(ref.get().thenCombine(async(ee, statsList), this::arangoStats)));
				return ref.get();
			});
		}).list();
		CompletableFuture.allOf(l.toArray(new CompletableFuture[l.size()]));
		s().stats(statsList);
	}

	private static final String EDGE_AQL = "upsert data._key == @key insert @data update {} in @col";

	private CompletableFuture<ArangoCursorAsync<BaseDocument>> async(Message v, List<Map<String, Object>> statsList) {
		Map<String, Object> d = v.map();
		CompletableFuture<ArangoCursorAsync<BaseDocument>> f = conn.client().db().query(EDGE_AQL, Maps.of("data", d, "col", v.table(),
				"key", v.key()), null, BaseDocument.class);
		statsList.add(d);
		return f;
	}
}
