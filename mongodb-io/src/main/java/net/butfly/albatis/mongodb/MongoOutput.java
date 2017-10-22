package net.butfly.albatis.mongodb;

import static net.butfly.albacore.utils.collection.Streams.list;
import static net.butfly.albacore.utils.collection.Streams.map;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import net.butfly.albacore.base.Namedly;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.Output;

public final class MongoOutput extends Namedly implements Output<Message> {
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
		open();
	}

	protected boolean enqueue(DBObject dbo) {
		if (null == dbo) return false;
		return 1 == (upsert ? collection.save(dbo).getN() : collection.insert(dbo).getN());
	}

	@Override
	public void enqueue(Stream<Message> msgs) {
		succeeded(upsert ? map(msgs, m -> collection.save(MongoConnection.dbobj(m)).getN(), Collectors.counting())
				: collection.insert(list(msgs, MongoConnection::dbobj)).getN());
	}
}
