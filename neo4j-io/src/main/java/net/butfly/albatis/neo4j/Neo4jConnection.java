package net.butfly.albatis.neo4j;

import java.io.IOException;
import java.util.List;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.GraphDatabase;

import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.Output;

public class Neo4jConnection extends NoSqlConnection<org.neo4j.driver.v1.Driver> {
	public Neo4jConnection(URISpec uri) throws IOException {
		super(uri, u -> GraphDatabase.driver(u.toString(), AuthTokens.basic(u.getUsername(), u.getPassword())), 7687, "bolt", "neo4j");
	}

	@Override
	public void close() throws IOException {
		client().close();
	}

	@Override
	public <M extends Message> Input<M> input(String... table) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <M extends Message> Output<M> output() throws IOException {
		throw new UnsupportedOperationException();
	}

	public static class Driver implements com.hzcominfo.albatis.nosql.Connection.Driver<Neo4jConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public Neo4jConnection connect(URISpec uriSpec) throws IOException {
			return new Neo4jConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("neo4j", "bolt");
		}
	}
}
