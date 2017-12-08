package net.butfly.albatis.mongodb;

import java.io.IOException;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Pair;
import net.butfly.albatis.io.OddOutput;

public final class MongoUpdateOutput extends net.butfly.albacore.base.Namedly implements OddOutput<Pair<DBObject, DBObject>> {
	private final boolean upsert;
	private final MongoConnection conn;
	private final DBCollection collection;

	public MongoUpdateOutput(String name, String uri) throws IOException {
		this(name, new MongoConnection(uri));
	}

	public MongoUpdateOutput(String name, MongoConnection conn) throws IOException {
		this(name, conn, conn.defaultCollection());
	}

	public MongoUpdateOutput(String name, String uri, String collection) throws IOException {
		this(name, uri, collection, true);
	}

	public MongoUpdateOutput(String name, MongoConnection conn, String collection) throws IOException {
		this(name, conn, collection, true);
	}

	public MongoUpdateOutput(String name, String uri, boolean upsert) throws IOException {
		this(name, new MongoConnection(uri), upsert);
	}

	public MongoUpdateOutput(String name, MongoConnection conn, boolean upsert) throws IOException {
		this(name, conn, conn.defaultCollection(), upsert);
	}

	public MongoUpdateOutput(String name, String uri, String collection, boolean upsert) throws IOException {
		this(name, new MongoConnection(new URISpec(uri)), collection, upsert);
	}

	public MongoUpdateOutput(String name, MongoConnection conn, String collection, boolean upsert) throws IOException {
		super(name);
		this.upsert = upsert;
		this.conn = conn;
		this.collection = this.conn.db().getCollection(collection);
		closing(this.conn::close);
		open();
	}

	@Override
	public boolean enqueue(Pair<DBObject, DBObject> queryAndUpdate) {
		if (null == queryAndUpdate) return false;
		WriteResult r = collection.update(queryAndUpdate.v1(), queryAndUpdate.v2(), true, upsert);
		return r.getN() > 0;
	}
}
