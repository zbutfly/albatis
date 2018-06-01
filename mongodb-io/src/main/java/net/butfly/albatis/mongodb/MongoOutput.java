package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.parallel.Lambdas;
import net.butfly.albatis.io.R;
import net.butfly.albatis.io.OutputBase;

public final class MongoOutput extends OutputBase<R> {
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

	protected boolean enqueue(DBObject dbo) {
		if (null == dbo) return false;
		return 1 == (upsert ? collection.save(dbo).getN() : collection.insert(dbo).getN());
	}

	@Override
	public void enqueue0(Sdream<R> msgs) {
		AtomicLong n = new AtomicLong();
		if (upsert) n.set(msgs.map(m -> conn.collection(m.table()).save(MongoConnection.dbobj(m)).getN()).reduce(Lambdas.sumInt()));
		else {
			Map<String, List<R>> l = Maps.of();
			msgs.eachs(m -> l.computeIfAbsent(m.table(), t -> Colls.list()).add(m));
			Exeter.of().join(e -> n.addAndGet(conn.collection(e.getKey()).insert(Sdream.of(e.getValue()).map(MongoConnection::dbobj).list()
					.toArray(new BasicDBObject[0])).getN()), l.entrySet());
		}
		succeeded(n.get());
	}
}
