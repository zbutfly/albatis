package net.butfly.albatis.arangodb;

import java.io.IOException;

import com.arangodb.entity.BaseDocument;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.io.OutputBase;

public class ArangoOutput extends OutputBase<AqlNestedMessage> {
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

	@Override
	protected void enqueue0(Sdream<AqlNestedMessage> edges) {
		edges.map(e -> e.exec(conn, s())).eachs(ArangoConnection::get);
	}

	@Override
	public Statistic trace() {
		return new Statistic(this).<BaseDocument> sizing(b -> conn.sizeOf(b)) //
				.<BaseDocument> sampling(BaseDocument::toString);
	}
}
