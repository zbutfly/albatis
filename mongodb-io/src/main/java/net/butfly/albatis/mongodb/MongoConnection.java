package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.net.UnknownHostException;
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
		super(new URISpec(connection), uri -> {
			MongoClientURI u = new MongoClientURI(uri.toString());
			try {
				return new MongoClient(u);
			} catch (UnknownHostException e) {
				throw new RuntimeException(e);
			}
		}, "mongodb");
		defaultDB = uri.getPathSegs()[0];
		dbs = new ConcurrentHashMap<>();
	}

	public DB db() {
		return db(defaultDB);
	}

	public DB db(String dbname) {
		return dbs.computeIfAbsent(dbname, n -> getClient().getDB(n));
	}

	@Override
	public void close() throws IOException {
		super.close();
		getClient().close();
	}
}
