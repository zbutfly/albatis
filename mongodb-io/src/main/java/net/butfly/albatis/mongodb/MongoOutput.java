package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.parallel.Lambdas;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.OutputBase;

public final class MongoOutput extends OutputBase<Message> {
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
	public void enqueue(Stream<Message> msgs) {
		succeeded(upsert ? msgs.parallel().map(m -> collection.save(MongoConnection.dbobj(m)).getN()).collect(Collectors.counting())
				: collection.insert(list(msgs, MongoConnection::dbobj)).getN());
	}
}
