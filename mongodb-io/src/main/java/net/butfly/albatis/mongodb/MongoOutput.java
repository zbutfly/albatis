package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.parallel.Lambdas;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public final class MongoOutput extends OutputBase<Rmap> {
	private static final long serialVersionUID = -6150620515173963739L;
	private final boolean upsert;
	private final MongoConnection conn;
	private final DBCollection collection;

	public MongoOutput(String name, MongoConnection conn) throws IOException {
		this(name, conn, conn.defaultCollection());
	}

	public MongoOutput(String name, MongoConnection conn, String collection) throws IOException {
		this(name, conn, collection, true);
	}

	public MongoOutput(String name, MongoConnection conn, boolean upsert) throws IOException {
		this(name, conn, conn.defaultCollection(), upsert);
	}

	public MongoOutput(String name, MongoConnection conn, String collection, boolean upsert) throws IOException {
		super(name);
		this.upsert = upsert;
		this.conn = conn;
		this.collection = this.conn.db().getCollection(collection);
		closing(this.conn::close);
	}

	@Override
	public URISpec target() {
		return conn.uri();
	}

	protected boolean enqueue(DBObject dbo) {
		if (null == dbo) return false;
		return 1 == (upsert ? collection.save(dbo).getN() : collection.insert(dbo).getN());
	}

	@Override
	protected void enqsafe(Sdream<Rmap> msgs) {
		AtomicLong n = new AtomicLong();
		if (upsert) n.set(msgs.map(m -> conn.collection(m.table()).save(MongoConnection.dbobj(m)).getN()).reduce(Lambdas.sumInt()));
		else Exeter.of().join(e -> n.addAndGet(conn.collection(e.getKey())//
				.insert(Sdream.of(e.getValue()).map(MongoConnection::dbobj).list().toArray(new BasicDBObject[0]))//
				.getN()), Maps.of(msgs, m -> m.table()).entrySet());
		succeeded(n.get());
	}
}
