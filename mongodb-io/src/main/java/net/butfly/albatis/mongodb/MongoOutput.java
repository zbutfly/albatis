package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.io.utils.Parals;

public final class MongoOutput extends Namedly implements Output<DBObject> {
	private final boolean upsert;
	private final MongoConnection conn;
	private final DBCollection collection;

	public MongoOutput(String name, String uri) throws IOException {
		this(name, new MongoConnection(uri));
	}

	public MongoOutput(String name, MongoConnection conn) throws IOException {
		this(name, conn, conn.defaultCollection());
	}

	public MongoOutput(String name, String uri, String collection) throws IOException {
		this(name, uri, collection, true);
	}

	public MongoOutput(String name, MongoConnection conn, String collection) throws IOException {
		this(name, conn, collection, true);
	}

	public MongoOutput(String name, String uri, boolean upsert) throws IOException {
		this(name, new MongoConnection(uri), upsert);
	}

	public MongoOutput(String name, MongoConnection conn, boolean upsert) throws IOException {
		this(name, conn, conn.defaultCollection(), upsert);
	}

	public MongoOutput(String name, String uri, String collection, boolean upsert) throws IOException {
		this(name, new MongoConnection(uri), collection, upsert);
	}

	public MongoOutput(String name, MongoConnection conn, String collection, boolean upsert) throws IOException {
		super(name);
		this.upsert = upsert;
		this.conn = conn;
		this.collection = this.conn.db().getCollection(collection);
		closing(this.conn::close);
		open();
	}

	protected boolean enqueue(DBObject dbo) {
		if (null == dbo) return false;
		return 1 == (upsert ? collection.save(dbo).getN() : collection.insert(dbo).getN());
	}

	@Override
	public long enqueue(Stream<DBObject> dbos) {
		if (upsert) {
			AtomicLong count = new AtomicLong();
			dbos.forEach(dbo -> {
				if (collection.save(dbo).getN() == 1) count.incrementAndGet();
			});
			return count.get();
		} else return collection.insert(Parals.list(dbos)).getN();
	}
}
