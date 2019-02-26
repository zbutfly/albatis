package net.butfly.albatis.mongodb;

import com.google.common.base.Joiner;
import com.mongodb.*;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;

import org.bson.BSONObject;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author butfly
 */
public class MongoConnection extends DataConnection<MongoClient> {
	final static String schema = "mongodb";
	private static final Logger logger = Logger.getLogger(MongoConnection.class);
	private final Map<String, DB> dbs;
	private final String defaultDB;
	private final String defaultCollection;

	public MongoConnection(URISpec urispec) throws IOException {
		super(urispec, "mongodb");
		if (uri.getPaths().length > 0) {
			defaultDB = uri.getPaths()[0];
			// Arrays.stream(uri.getFile().split(",")).filter(t -> !t.isEmpty()).toArray(i -> new String[i])
			defaultCollection = null != uri.getFile() ? uri.getFile() : null;
		} else if (null != uri.getFile()) {
			defaultDB = uri.getFile();
			defaultCollection = null;
		} else {
			defaultDB = null;
			defaultCollection = null;
		}
		dbs = Maps.of();
	}

	@Override
	protected MongoClient initialize(URISpec uri) {
		try {
			String str = uri.getScheme() + "://" + uri.getAuthority() + "/";
			String db = uri.getPathAt(0);
			if (null != db) str += db;
			return new MongoClient(new MongoClientURI(str));
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}

	@Deprecated
	public MongoConnection(String urispec) throws IOException {
		this(new URISpec(urispec));
	}

	public DB db() {
		return db(defaultDB);
	}

	public DB db(String dbname) {
		return dbs.computeIfAbsent(dbname, n -> client.getDB(n));
	}

	public DBCollection collection() {
		return db().getCollection(defaultCollection);
	}

	private final Map<String, DBCollection> COLS = Maps.of();

	public DBCollection collection(String collection) {
		return COLS.computeIfAbsent(collection, c -> {
			if (!db().collectionExists(collection)) {
				logger.info("Mongodb collection create on [" + db().toString() + "] with name: " + collection);
				db().createCollection(collection, dbobj());
			}
			return db().getCollection(collection);
		});
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
		client.close();
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

	public static class Driver implements net.butfly.albatis.Connection.Driver<MongoConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public MongoConnection connect(URISpec uriSpec) throws IOException {
			return new MongoConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("mongodb");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public MongoInput inputRaw(TableDesc... table) throws IOException {
		MongoInput i = new MongoInput("MongoInput", this);
		List<String> l = Colls.list(t -> t.name, table);
		i.table(l.toArray(new String[l.size()]));
		return i;
	}

	@SuppressWarnings("unchecked")
	@Override
	public MongoOutput outputRaw(TableDesc... table) throws IOException {
		return new MongoOutput("MongoOutput", this);
	}

	@Override
	public void construct(String table, TableDesc tableDesc, List<FieldDesc> fields) {
		String[] tables = table.split("\\.");
		String dbName, tableName;
		if (tables.length == 1)
			dbName = tableName = tables[0];
		else if((tables.length == 2)) {
			dbName = tables[0];
			tableName = tables[1];
		}else throw new RuntimeException("Please type in corrent es table format: db.table !");
		DB db = client.getDB(dbName);
		DBObject object = new BasicDBObject();
		Object cappedSize = tableDesc.construct.get("size");
		Object cappedMax = tableDesc.construct.get("max");
		object.put("capped", true);
		object.put("size", cappedSize);
		object.put("max", cappedMax);
		db.createCollection(table, object);
	}

	@Override
	public boolean judge(String table) {
		String[] tables = table.split("\\.");
		String dbName, tableName;
		if (tables.length == 1)
			dbName = tableName = tables[0];
		else if((tables.length == 2)) {
			dbName = tables[0];
			tableName = tables[1];
		}else throw new RuntimeException("Please type in corrent es table format: db.table !");
		DB db = client.getDB(dbName);
		return db.getCollectionNames().contains(tableName);
	}
}
