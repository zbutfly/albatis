package net.butfly.albatis.arangodb;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.arangodb.entity.BaseDocument;

import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.logger.Statistic;

public class AqlEdgeMessage extends AqlVertexMessage {
	private static final long serialVersionUID = -7016058550224590870L;
	public final AqlVertexMessage start, end;

	public AqlEdgeMessage(String tbl, Object key, Map<String, Object> vertex, AqlVertexMessage start, AqlVertexMessage end) {
		super(tbl, key, vertex);
		this.start = start;
		this.end = end;
	}

	@Override
	public CompletableFuture<List<BaseDocument>> exec(ArangoConnection conn, Statistic s) {
		return start.exec(conn, s)//
				.thenCombineAsync(end.exec(conn, s), ArangoConnection::merge, Exeter.of())//
				.thenComposeAsync(l -> super.exec(conn, s), Exeter.of());
	}

	@Override
	public String toString() {
		return super.toString() + ", start: " + start.toString() + ", end: " + end.toString();
	}
}
