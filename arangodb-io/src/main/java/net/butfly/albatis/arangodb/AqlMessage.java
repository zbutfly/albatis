package net.butfly.albatis.arangodb;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.arangodb.entity.BaseDocument;

import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.logger.Statistic;

public class AqlMessage extends AqlNestedMessage {
	private static final long serialVersionUID = 6783506759570424659L;
	private final String aql;

	public AqlMessage(String aql) {
		super();
		this.aql = aql;
	}

	@Override
	public CompletableFuture<List<BaseDocument>> exec(ArangoConnection conn, Statistic s) {
		return conn.exec(aql, null, s).thenComposeAsync(l -> {
			nestedResults(l);
			return super.exec(conn, s);
		}, Exeter.of());
	}
}
