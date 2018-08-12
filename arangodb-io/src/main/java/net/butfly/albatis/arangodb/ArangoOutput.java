package net.butfly.albatis.arangodb;

import java.io.IOException;

import com.arangodb.entity.BaseDocument;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.io.OutputBase;

public class ArangoOutput extends OutputBase<AqlNestedMessage> {
	private static final long serialVersionUID = -2376114954650957250L;
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
	protected void enqsafe(Sdream<AqlNestedMessage> edges) {
		edges.map(e -> e.exec(conn, s())).eachs(ArangoConnection::get);
	}

	@Override
	public Statistic trace() {
		return new Statistic(this).<BaseDocument> sizing(b -> conn.sizeOf(b)) //
				.<BaseDocument> sampling(BaseDocument::toString);
	}

	@Override
	public URISpec target() {
		return conn.uri();
	}
}
