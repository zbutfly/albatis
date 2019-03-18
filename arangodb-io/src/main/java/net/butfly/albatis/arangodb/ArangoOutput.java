package net.butfly.albatis.arangodb;

import static net.butfly.albatis.io.IOProps.propI;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import com.arangodb.entity.BaseDocument;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.io.OutputBase;

public class ArangoOutput extends OutputBase<AqlNestedMessage> {
	private static final long serialVersionUID = -2376114954650957250L;
	private final ArangoConnection conn;
	private static final int MAX_RETRY = propI(ArangoOutput.class, "retry.max", 50); // 3M

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

	@Override
	protected void enqsafe(Sdream<AqlNestedMessage> edges) {
		Map<AqlNestedMessage, Pair<Integer, CompletableFuture<List<BaseDocument>>>> rs = Maps.of();
		for (AqlNestedMessage m : edges.list()) {
			for (int i = 0; MAX_RETRY > 0 && i < MAX_RETRY; i++)
				rs.put(m, new Pair<>(0, m.exec(conn, s())));
		}
		while (!rs.isEmpty()) {
			Entry<AqlNestedMessage, Pair<Integer, CompletableFuture<List<BaseDocument>>>> first = rs.entrySet()
					.iterator().next();
			result(rs, first.getKey(), first.getValue().v1() + 1, first.getValue().v2());
		}
	}

	private void result(Map<AqlNestedMessage, Pair<Integer, CompletableFuture<List<BaseDocument>>>> rs,
			AqlNestedMessage m, Integer retry, CompletableFuture<List<BaseDocument>> f) {
		List<BaseDocument> r = ArangoConnection.get(f);
		if (null != r) {
			if (retry > 5)
				logger().warn("Many retries [" + retry + "] and success: " + r.toString());
			rs.remove(m);
		} else {
			if (retry > MAX_RETRY) {
				logger().error("Fail after retry [" + MAX_RETRY + "]: " + m.toString());
				rs.remove(m);
			} else {// retry
				if (retry > 5)
					logger().warn("Many retries [" + retry + "], still not success: " + m.toString());
				rs.put(m, new Pair<>(retry, m.exec(conn, s())));
			}
		}
	}

	@Override
	public Statistic trace() {
		return new Statistic(this).<BaseDocument>sizing(b -> conn.sizeOf(b)) //
				.<BaseDocument>sampling(BaseDocument::toString);
	}

	@Override
	public URISpec target() {
		return conn.uri();
	}
}
