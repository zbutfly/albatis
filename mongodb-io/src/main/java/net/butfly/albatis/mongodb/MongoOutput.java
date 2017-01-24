package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.util.List;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import net.butfly.albacore.io.OutputImpl;
import net.butfly.albacore.utils.Collections;

public class MongoOutput extends OutputImpl<DBObject> {
	private final boolean upsert;
	private final MongoConnection conn;
	private final DBCollection collection;

	public MongoOutput(String name, String uri, String collection, boolean upsert) throws IOException {
		super(name);
		this.upsert = upsert;
		this.conn = new MongoConnection(uri);
		this.collection = conn.db().getCollection(collection);
	}

	@Override
	public void close() {
		super.close(conn::close);
	}

	@Override
	public boolean enqueue(DBObject dbo, boolean block) {
		if (null == dbo) return false;
		if (!upsert) return collection.insert(dbo).getN() == 1;
		else return collection.save(dbo).getN() == 1;
	}

	@Override
	public long enqueue(List<DBObject> dbos) {
		return collection.insert(Collections.noNull(dbos)).getN();
	}
}
