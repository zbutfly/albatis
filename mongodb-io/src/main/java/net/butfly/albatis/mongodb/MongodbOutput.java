package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.util.Iterator;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import net.butfly.albacore.io.OutputQueue;
import net.butfly.albacore.io.OutputQueueImpl;
import net.butfly.albacore.utils.Collections;
import net.butfly.albatis.mongodb.Mongos.MDB;

public class MongodbOutput extends OutputQueueImpl<DBObject, DBObject> implements OutputQueue<DBObject> {
	private static final long serialVersionUID = 2141020043117686747L;
	private final boolean upsert;
	private final MDB mdb;
	private final DBCollection collection;

	public MongodbOutput(final String table, final String configFile, boolean upsert, final boolean continuous) throws IOException {
		super("mongodb-output-queue");
		this.upsert = upsert;
		this.mdb = Mongos.mongoConnect(configFile);
		this.collection = mdb.db().getCollection(table);
	}

	@Override
	public void close() {
		mdb.close();
	}

	@Override
	protected boolean enqueueRaw(DBObject r) {
		if (!upsert) return collection.insert(r).getN() == 1;
		else return collection.save(r).getN() == 1;
	}

	@Override
	public long enqueue(Iterator<DBObject> iter) {
		return collection.insert(Collections.asList(iter)).getN();
	}

	@Override
	public long enqueue(Iterable<DBObject> it) {
		return collection.insert(Collections.asList(it)).getN();
	}

	@Override
	public long enqueue(DBObject... r) {
		return collection.insert(r).getN();
	}
}
