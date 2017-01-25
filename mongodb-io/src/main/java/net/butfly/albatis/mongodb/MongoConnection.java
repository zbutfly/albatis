package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.hzcominfo.albatis.nosql.NoSqlConnection;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.logger.Logger;

public class MongoConnection extends NoSqlConnection<MongoClient> {
	private static final Logger logger = Logger.getLogger(MongoConnection.class);
	private final Map<String, DB> dbs;
	private final String defaultDB;
	private final String defaultCollection;

	public MongoConnection(URISpec urispec) throws IOException {
		super(urispec, u -> {
			try {
				return new MongoClient(new MongoClientURI(u.getScheme() + "://" + u.getAuthority() + "/" + u.getPathSegs()[0]));
			} catch (UnknownHostException e) {
				throw new RuntimeException(e);
			}
		}, "mongodb");
		defaultDB = uri.getPathSegs()[0];
		defaultCollection = uri.getPathSegs().length > 1 ? uri.getPathSegs()[1] : null;
		dbs = new ConcurrentHashMap<>();
	}

	public MongoConnection(String urispec) throws IOException {
		this(new URISpec(urispec));
	}

	public DB db() {
		return db(defaultDB);
	}

	public DB db(String dbname) {
		return dbs.computeIfAbsent(dbname, n -> client().getDB(n));
	}

	public DBCollection collection() {
		return db().getCollection(defaultCollection);
	}

	public DBCollection collection(String collection) {
		return db().getCollection(collection);
	}

	public DBCollection collection(String dbname, String collection) {
		return db(dbname).getCollection(collection);
	}

	@Override
	public void close() {
		try {
			super.close();
		} catch (IOException e) {
			logger.error("Close failure", e);
		}
		client().close();
	}

	public String defaultCollection() {
		return defaultCollection;
	}
}
