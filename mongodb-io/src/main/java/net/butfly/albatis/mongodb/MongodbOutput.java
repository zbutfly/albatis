package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.util.Iterator;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import net.butfly.albacore.io.OutputQueue;
import net.butfly.albacore.io.OutputQueueImpl;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.Systems;
import net.butfly.albatis.mongodb.Mongos.MDB;

public class MongodbOutput<T> extends OutputQueueImpl<T, DBObject> implements OutputQueue<T> {
	private static final long serialVersionUID = 2141020043117686747L;
	private final boolean upsert;
	private final MDB mdb;
	private final DBCollection collection;

	public MongodbOutput(String table, final String configFile, final Converter<T, DBObject> conv, final boolean upsert)
			throws IOException {
		super("mongodb-output-queue", conv);
		this.upsert = upsert;
		this.mdb = Mongos.connect(configFile);
		this.collection = mdb.db().getCollection(Systems.suffixDebug(table, logger));
	}

	@Override
	public void close() {
		mdb.close();
	}

	@Override
	protected boolean enqueueRaw(T r) {
		DBObject dbo = conv.apply(r);
		if (null == dbo) return false;
		if (!upsert) return collection.insert(dbo).getN() == 1;
		else return collection.save(dbo).getN() == 1;
	}

	@Override
	public long enqueue(Iterator<T> iter) {
		return collection.insert(Collections.transform(iter, conv)).getN();
	}
}
