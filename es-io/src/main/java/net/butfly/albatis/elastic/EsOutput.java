package net.butfly.albatis.elastic;

import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.io.IOException;
import java.util.stream.Stream;

import org.elasticsearch.transport.RemoteTransportException;

import com.hzcominfo.albatis.nosql.Connection;

import net.butfly.albacore.io.faliover.Failover.FailoverException;
import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.io.utils.URISpec;

public final class EsOutput extends FailoverOutput<String, ElasticMessage> {
	private final ElasticConnect conn;

	public EsOutput(String name, String uri, String failoverPath) throws IOException {
		this(name, new URISpec(uri), failoverPath);
	}

	public EsOutput(String name, URISpec uri, String failoverPath) throws IOException {
		super(name, b -> new ElasticMessage(b), failoverPath, Integer.parseInt(uri.getParameter(Connection.PARAM_KEY_BATCH, "200")));
		conn = new ElasticConnection(uri);
		open();
	}

	public ElasticConnect getConnection() {
		return conn;
	}

	@Override
	protected void closeInternal() {
		try {
			conn.close();
		} catch (Exception e) {}
	}

	@Override
	protected long write(String type, Stream<ElasticMessage> msgs) throws FailoverException {
		return conn.update(type, msgs.toArray(i -> new ElasticMessage[i]));
	}

	static {
		unwrap(RemoteTransportException.class, "getCause");
	}
}
