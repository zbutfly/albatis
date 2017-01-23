package net.butfly.albatis.mongodb;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.utils.Collections;

import java.io.IOException;
import java.util.List;

public class MongoOutput extends Output<DBObject> {
	private static final long serialVersionUID = 2141020043117686747L;
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
	public boolean enqueue0(DBObject dbo) {
		if (null == dbo) return false;
		if (!upsert) return collection.insert(dbo).getN() == 1;
		else return collection.save(dbo).getN() == 1;
	}

	@Override
	public long enqueue(List<DBObject> dbos) {
		return collection.insert(Collections.noNull(dbos)).getN();
	}
}
