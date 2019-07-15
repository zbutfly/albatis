package net.butfly.albatis.arangodb;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.arangodb.entity.BaseDocument;

import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Rmap;

public abstract class AqlNestedMessage extends Rmap {
	private static final long serialVersionUID = 6783506759570424659L;
	private List<BaseDocument> nestedResults = null;
	private Function<Map<String, Object>, List<AqlNestedMessage>> nestedThen;

	protected AqlNestedMessage() {
		super();
	}

	protected AqlNestedMessage(String tbl, Object key, Map<String, Object> vertex) {
		super(tbl, key, vertex);
	}

	public CompletableFuture<List<BaseDocument>> exec(ArangoConnection conn) {
		if (null == nestedResults) throw new IllegalStateException("Parent of nested aql has not been executed.");
		List<CompletableFuture<List<BaseDocument>>> fff = Colls.list();
		for (BaseDocument d : nestedResults)
			for (AqlNestedMessage n : applyThen(d))
				fff.add(n.exec(conn));
		return fff.isEmpty() ? ArangoConnection.empty() : ArangoConnection.merge(fff);
	}

	public void then(Function<Map<String, Object>, List<AqlNestedMessage>> nestedThen) {
		this.nestedThen = nestedThen;
	}

	protected final void nestedResults(List<BaseDocument> docs) {
//		if (null != nestedResults) throw new IllegalStateException("Parent of nested aql has been executed more than once.");
		nestedResults = docs;
	}

	protected final List<AqlNestedMessage> applyThen(BaseDocument doc) {
		if (null == nestedThen) return Colls.list();
		if (null == doc) return nestedThen.apply(Maps.of());
		Map<String, Object> m = doc.getProperties();
		m.put("_key", doc.getKey());// fuck, no _key in props
		return nestedThen.apply(m);
	}
}
