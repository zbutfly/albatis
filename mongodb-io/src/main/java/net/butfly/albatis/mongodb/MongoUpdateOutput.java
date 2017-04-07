package net.butfly.albatis.mongodb;

import java.io.IOException;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

import net.butfly.albacore.io.OutputImpl;
import net.butfly.albacore.utils.Pair;

public final class MongoUpdateOutput extends OutputImpl<Pair<DBObject, DBObject>> {
	private final boolean upsert;
	private final MongoConnection conn;
	private final DBCollection collection;

	public MongoUpdateOutput(String name, String uri, String collection) throws IOException {
		this(name, uri, collection, true);
	}

	public MongoUpdateOutput(String name, String uri, String collection, boolean upsert) throws IOException {
		super(name);
		this.upsert = upsert;
		this.conn = new MongoConnection(uri);
		this.collection = conn.db().getCollection(collection);
		closing(conn::close);
		open();
	}

	@Override
	protected boolean enqueue(Pair<DBObject, DBObject> queryAndUpdate) {
		if (null == queryAndUpdate) return false;
		WriteResult r = collection.update(queryAndUpdate.v1(), queryAndUpdate.v2(), true, upsert);
		return r.getN() > 0;
	}
}
