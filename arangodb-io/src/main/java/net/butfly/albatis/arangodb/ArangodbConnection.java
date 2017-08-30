package net.butfly.albatis.arangodb;

import java.io.IOException;
import java.util.function.Function;

import com.arangodb.ArangoDB;
import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.io.URISpec;

public class ArangodbConnection extends NoSqlConnection<ArangoDB> {
	public ArangodbConnection(URISpec uri, Function<URISpec, ArangoDB> client) throws IOException {
		super(uri, u -> {
			return new ArangoDB.Builder().host("192.168.182.50", 8888).user(u.getUsername()).password(u.getPassword()).build();
		}, 8529, "arango", "arangodb");
	}

	@Override
	public void close() throws IOException {
		client().shutdown();
	}
}
