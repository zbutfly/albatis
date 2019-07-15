package net.butfly.albatis.arangodb;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.arangodb.entity.BaseDocument;

import net.butfly.albacore.paral.Exeter;

public class AqlEdgeMessage extends AqlVertexMessage {
	private static final long serialVersionUID = -7016058550224590870L;
	public final AqlVertexMessage start, end;

	public AqlEdgeMessage(String tbl, Object key, Map<String, Object> vertex, AqlVertexMessage start, AqlVertexMessage end) {
		super(tbl, key, vertex);
		this.start = start;
		this.end = end;
	}

	@Override
	public CompletableFuture<List<BaseDocument>> exec(ArangoConnection conn) {
		if (null == start && null == end) return super.exec(conn);
		else return start.exec(conn).thenCombineAsync(end.exec(conn), ArangoConnection::merge, Exeter.of())//
				.thenComposeAsync(l -> super.exec(conn), Exeter.of());
	}

	@Override
	public String toString() {
		StringBuilder rs = new StringBuilder(super.toString());
		if (null != start) rs.append(", start: ").append(start.toString());
		if (null != end) rs.append(", end: ").append(end.toString());
		return rs.toString();
	}
}
