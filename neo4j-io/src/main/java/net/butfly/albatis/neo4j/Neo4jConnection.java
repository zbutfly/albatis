package net.butfly.albatis.neo4j;

import java.io.IOException;
import java.util.function.Function;

import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.io.utils.URISpec;

public class Neo4jConnection extends NoSqlConnection<Driver> {
	public Neo4jConnection(URISpec uri, Function<URISpec, Driver> client) throws IOException {
		super(uri, u -> {
			AuthToken auth = AuthTokens.basic(u.getUsername(), u.getPassword());
			return GraphDatabase.driver(u.toString(), auth);
		}, 7687, "bolt", "neo4j");
	}

	@Override
	public void close() throws IOException {
		client().close();
	}
}
