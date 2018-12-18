package net.butfly.albatis.neo4j;

import java.io.IOException;
import java.util.List;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.GraphDatabase;

import com.hzcominfo.albatis.nosql.DataConnection;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;

public class Neo4jConnection extends DataConnection<org.neo4j.driver.v1.Driver> {
	public Neo4jConnection(URISpec uri) throws IOException {
		super(uri, 7687, "neo4j", "bolt");
	}

	@Override
	protected org.neo4j.driver.v1.Driver initialize(URISpec uri) {
		return GraphDatabase.driver(uri.toString(), AuthTokens.basic(uri.getUsername(), uri.getPassword()));
	}

	@Override
	public void close() throws IOException {
		client.close();
	}

	@Override
	public <M extends Rmap> Input<M> createInput(TableDesc... table) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <M extends Rmap> Output<M> output(TableDesc... table) throws IOException {
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
