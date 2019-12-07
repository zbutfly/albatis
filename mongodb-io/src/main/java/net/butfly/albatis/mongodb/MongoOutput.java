package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoWaitQueueFullException;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public class MongoOutput extends OutputBase<Rmap> {
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
		this.collection = null == collection ? null : this.conn.db().getCollection(collection);
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
		List<Rmap> retries = Colls.list();
		if (upsert) msgs.eachs(m -> upsert(m, m.keyField(), m.key(), retries));
		else Exeter.of().join(e -> n.addAndGet(conn.collection(e.getKey().name).insert(//
				Sdream.of(e.getValue()).map(MongoConnection::dbobj).list().toArray(new BasicDBObject[0])//
		).getN()), Maps.of(msgs, m -> m.table()).entrySet());
		succeeded(n.get());
		if (!retries.isEmpty()) failed(Sdream.of(retries));
	}

	private boolean upsert(Rmap m, String keyField, Object keyValue, List<Rmap> retries) {
		boolean retry;
		if (null == keyField && null != keyValue) conn.collection(m.table().name).save(MongoConnection.dbobj(m)).getN();
		else if (null == keyField && null == keyValue) throw new IllegalArgumentException("Please check your configuration that insured that the key field is configured!");
		else do {
			retry = false;
			try {
				conn.collection(m.table().name).findAndModify(new BasicDBObject(keyField, keyValue), //
						null, null, false, MongoConnection.dbobj(m), false, true);
			} catch (MongoWaitQueueFullException e) {
				retry = true;
			} catch (MongoCommandException e) {
				if (11000 == e.getErrorCode()) retries.add(m);
				else logger().error("upsert mongo error!", e);
				return false;
			}
		} while (retry);
		return true;
	}
}
