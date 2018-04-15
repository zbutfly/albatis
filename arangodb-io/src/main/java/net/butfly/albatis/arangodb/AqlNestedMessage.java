package net.butfly.albatis.arangodb;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.arangodb.entity.BaseDocument;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.io.Message;

public abstract class AqlNestedMessage extends Message {
	private static final long serialVersionUID = 6783506759570424659L;
	private List<BaseDocument> nestedResults = null;
	private Function<Map<String, Object>, List<AqlNestedMessage>> nestedThen;

	protected AqlNestedMessage() {
		super();
	}

	protected AqlNestedMessage(String tbl, Object key, Map<String, Object> vertex) {
		super(tbl, key, vertex);
		vertex.put("_key", key);
	}

	public CompletableFuture<List<BaseDocument>> exec(ArangoConnection conn, Statistic s) {
		if (null == nestedResults) throw new IllegalStateException("Parent of nested aql has not been executed.");
		List<CompletableFuture<List<BaseDocument>>> fff = Colls.list();
		for (BaseDocument d : nestedResults)
			for (AqlNestedMessage n : applyThen(d))
				fff.add(n.exec(conn, s));
		return fff.isEmpty() ? ArangoConnection.empty() : ArangoConnection.merge(fff);
	}

	public void then(Function<Map<String, Object>, List<AqlNestedMessage>> nestedThen) {
		this.nestedThen = nestedThen;
	}

	protected final void nestedResults(List<BaseDocument> docs) {
		if (null != nestedResults) throw new IllegalStateException("Parent of nested aql has been executed more than once.");
		nestedResults = docs;
	}

	protected final List<AqlNestedMessage> applyThen(BaseDocument doc) {
		return null == nestedThen ? Colls.list() : nestedThen.apply(null == doc ? Maps.of() : doc.getProperties());
	}
}
