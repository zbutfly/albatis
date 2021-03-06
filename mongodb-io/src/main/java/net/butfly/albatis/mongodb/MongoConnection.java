package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.bson.BSONObject;

import com.google.common.base.Joiner;
import com.hzcominfo.albatis.nosql.NoSqlConnection;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.logger.Logger;

/**
 * @author butfly
 *
 */
public class MongoConnection extends NoSqlConnection<MongoClient> {
	private static final Logger logger = Logger.getLogger(MongoConnection.class);
	private final Map<String, DB> dbs;
	private final String defaultDB;
	private final String defaultCollection;

	public MongoConnection(URISpec urispec) throws IOException {
		super(urispec, u -> {
			try {
				String str = u.getScheme() + "://" + u.getAuthority() + "/";
				String db = u.getPathAt(0);
				if (null != db) str += db;
				return new MongoClient(new MongoClientURI(str));
			} catch (UnknownHostException e) {
				throw new RuntimeException(e);
			}
		}, "mongodb");
		defaultDB = uri.getPathAt(0);
		defaultCollection = uri.getPathAt(1);
		dbs = new ConcurrentHashMap<>();
	}

	@Deprecated
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
		if (!db().collectionExists(collection)) {
			logger.debug("Mongodb collection create on [" + db().toString() + "] with name: " + collection);
			db().createCollection(collection, dbobj());
		}
		return db().getCollection(collection);
	}

	public boolean collectionExists(String collectionName) {
		return db().collectionExists(collectionName);
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

	public static BasicDBObject dbobj() {
		return new BasicDBObject();
	}

	/**
	 * Deeply clone
	 * 
	 * @param origin
	 * @return
	 */
	public static BasicDBObject dbobj(BSONObject... origin) {
		BasicDBObject dbo = new BasicDBObject();
		for (BSONObject o : origin)
			if (null != o) for (String k : o.keySet())
				putDeeply(dbo, k, o.get(k));
		return dbo;
	}

	/**
	 * Deeply clone
	 * 
	 * @param map
	 * @return
	 */
	public static BasicDBObject dbobj(Map<String, ?> map) {
		BasicDBObject dbo = new BasicDBObject();
		for (String k : map.keySet())
			putDeeply(dbo, k, map.get(k));
		return dbo;
	}

	@SuppressWarnings("unchecked")
	private static void putDeeply(BasicDBObject dbo, String k, Object v) {
		if (null == v) dbo.put(k, v);
		else if (v instanceof BSONObject) dbo.put(k, dbobj((BSONObject) v));
		else if (v instanceof Map) dbo.put(k, dbobj((Map<String, ?>) v));
		else dbo.put(k, v);
	}

	public static BasicDBObject dbobj(String key, Object... valueAndKeys) {
		BasicDBObject dbo = dbobj();
		if (null != valueAndKeys[0]) dbo.put(key, valueAndKeys[0]);
		for (int i = 1; i + 1 < valueAndKeys.length; i += 2)
			if (null != valueAndKeys[i + 1]) dbo.put(((CharSequence) valueAndKeys[i]).toString(), valueAndKeys[i + 1]);
		return dbo;
	}

	public static BasicDBList dblist(Object first, Object... others) {
		BasicDBList dbl = new BasicDBList();
		dbl.add(first);
		dbl.addAll(Arrays.asList(others));
		return dbl;
	}

	public DBCursor cursor(String table, DBObject... filter) {
		DBCursor cursor;
		if (!collectionExists(table)) throw new IllegalArgumentException("Collection [" + table + "] not existed for input");
		DBCollection col = collection(table);
		long now;
		if (null == filter || filter.length == 0) {
			now = System.nanoTime();
			cursor = col.find();
		} else {
			logger.info("Mongodb [" + table + "] filters: \n\t" + Joiner.on("\n\t").join(filter) + "\nnow count:");
			if (filter.length == 1) {
				now = System.nanoTime();
				cursor = col.find(filter[0]);
			} else {
				BasicDBList filters = new BasicDBList();
				for (DBObject f : filter)
					filters.add(f);
				now = System.nanoTime();
				cursor = col.find(dbobj("$and", filters));
			}
		}
		String p = getParameter("limit");
		if (p != null) cursor.limit(Integer.parseInt(p));
		p = getParameter("skip");
		if (p != null) cursor.skip(Integer.parseInt(p));
		int count = cursor.count();
		logger.debug(() -> "Mongodb [" + table + "] find [" + count + " records], end in [" + (System.nanoTime() - now) / 1000 + " ms].");
		logger.trace(() -> "Mongodb [" + table + "] find [" + cursor.size() + " records].");
		return cursor;
	}
}
