package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.hzcominfo.albatis.nosql.NoSqlConnection;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import net.butfly.albacore.io.URISpec;

public class MongoConnection extends NoSqlConnection<MongoClient> {
	private final Map<String, DB> dbs;
	private String defaultDB;

	public MongoConnection(String connection) throws IOException {
		super(connection, "mongodb");
		dbs = new ConcurrentHashMap<>();
	}

	@Override
	protected MongoClient createClient(URISpec url) throws IOException {
		MongoClientURI u = new MongoClientURI(uri.toString());
		defaultDB = u.getDatabase();
		MongoClient mongo = new MongoClient(u);
		return mongo;
	}

	public DB db() {
		return db(defaultDB);
	}

	public DB db(String dbname) {
		return dbs.computeIfAbsent(dbname, n -> client().getDB(n));
	}

	@Override
	public void close() throws IOException {
		super.close();
		client().close();
	}
}
