package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.util.Iterator;

import com.mongodb.BasicDBList;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import net.butfly.albacore.io.OutputQueue;
import net.butfly.albacore.io.OutputQueueImpl;
import net.butfly.albacore.utils.Systems;
import net.butfly.albatis.mongodb.Mongos.MConnection;

public class MongodbOutput extends OutputQueueImpl<DBObject> implements OutputQueue<DBObject> {
	private static final long serialVersionUID = 2141020043117686747L;
	private final boolean upsert;
	private final MConnection mdb;
	private final DBCollection collection;

	public MongodbOutput(String name, String configFile, String table, boolean upsert) throws IOException {
		super(name);
		this.upsert = upsert;
		this.mdb = Mongos.connect(configFile);
		this.collection = mdb.connection().getCollection(Systems.suffixDebug(table, logger));
	}

	@Override
	public void close() {
		mdb.close();
	}

	@Override
	protected boolean enqueueRaw(DBObject dbo) {
		if (null == dbo) return false;
		if (!upsert) return collection.insert(dbo).getN() == 1;
		else return collection.save(dbo).getN() == 1;
	}

	@Override
	public long enqueue(Iterator<DBObject> iter) {
		BasicDBList l = new BasicDBList();
		while (iter.hasNext())
			l.add(iter.next());
		return collection.insert(l).getN();
	}
}
