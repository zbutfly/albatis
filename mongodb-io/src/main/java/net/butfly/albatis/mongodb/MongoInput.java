package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Joiner;
import com.mongodb.Bytes;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.OddInput;

public class MongoInput extends net.butfly.albacore.base.Namedly implements OddInput<Message> {
	private static final Logger logger = Logger.getLogger(MongoInput.class);
	private final MongoConnection conn;
	private final BlockingQueue<Pair<String, DBCursor>> cursors;
	private final AtomicInteger cursorCount;

	public MongoInput(String name, MongoConnection conn) throws IOException {
		this(name, conn, conn.defaultCollection());
	}

	public MongoInput(String name, MongoConnection conn, String... tables) throws IOException {
		super(name);
		this.conn = conn;
		if (null == tables) {
			if (conn.defaultCollection() == null) throw new IOException("MongoDB could not input whith non or null table.");
			tables = new String[] { conn.defaultCollection() };
		}
		logger.info("[" + name + "] from [" + conn.toString() + "], collection [" + Joiner.on(",").join(tables) + "]");
		cursors = new LinkedBlockingQueue<>(tables.length);
		List<Runnable> queries = Colls.list();
		for (String t : tables)
			queries.add(() -> {
				try {
					DBCursor c = conn.cursor(t).batchSize(conn.getBatchSize()).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
					if (c.hasNext()) {
						cursors.add(new Pair<>(t, c));
						if (logger.isDebugEnabled()) logger.info("MongoDB query [" + t + "] successed, count: [" + c.count() + "].");
						else logger.info("MongoDB query [" + t + "] successed.");
					} else logger.warn("MongoDB query [" + t + "] finished but empty.");
				} catch (Exception e) {
					logger.error("MongoDB query [" + t + "] failed", e);
				}
			});
		Exeter.of().join(queries.toArray(new Runnable[queries.size()]));
		cursorCount = new AtomicInteger(cursors.size());
		closing(this::closeMongo);
		trace(DBObject.class).sizing(b -> (long) b.keySet().size()).detailing(() -> "[MongoDB stats field count, not bytes]");
		open();
	}

	public MongoInput(String name, MongoConnection conn, Map<String, DBObject> tablesAndQueries) throws IOException {
		super(name);
		this.conn = conn;
		if (null == tablesAndQueries) {
			if (conn.defaultCollection() == null) throw new IOException("MongoDB could not input whith non or null table.");
			tablesAndQueries = Maps.of();
			tablesAndQueries.put(conn.defaultCollection(), MongoConnection.dbobj());
		}
		logger.info("[" + name + "] from [" + conn.toString() + "], collection [" + tablesAndQueries.toString() + "]");
		cursors = new LinkedBlockingQueue<>(tablesAndQueries.size());
		List<Runnable> queries = Colls.list();
		for (Map.Entry<String, DBObject> e : tablesAndQueries.entrySet())
			queries.add(() -> {
				try {
					DBCursor c = conn.cursor(e.getKey(), e.getValue()).batchSize(conn.getBatchSize()).addOption(
							Bytes.QUERYOPTION_NOTIMEOUT);
					if (c.hasNext()) {
						cursors.add(new Pair<>(e.getKey(), c));
						logger.info("MongoDB query [" + e.getKey() + "] successed, count: [" + c.count() + "].");
					} else logger.warn("MongoDB query [" + e.getKey() + "] finished but empty.");
				} catch (Exception ex) {
					logger.error("MongoDB query [" + e.getKey() + "] failed", ex);
				}
			});
		Exeter.of().join(queries.toArray(new Runnable[queries.size()]));
		cursorCount = new AtomicInteger(cursors.size());
		closing(this::closeMongo);
		open();
	}

	private void closeMongo() {
		while (!cursors.isEmpty())
			closeCursor(cursors.poll());
		conn.close();
	}

	private Pair<String, DBCursor> closeCursor(Pair<String, DBCursor> c) {
		try {
			c.v2().close();
			logger.info("MongoDB Cursor of [" + c.v1() + "] closed, remained valid cursor: " + cursorCount.decrementAndGet());
			return null;
		} catch (Exception e) {
			logger.error("MongoDB cursor of [" + c.v1() + "] close fail", e);
			return c;
		}
	}

	@Override
	public boolean empty() {
		return cursorCount.get() <= 0;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Message dequeue() {
		Pair<String, DBCursor> c;
		while (opened() && !empty())
			while (null != (c = cursors.poll()))
				try {
					if (!c.v2().hasNext()) c = closeCursor(c);
					else {
						@SuppressWarnings("rawtypes")
						Map m = stats(c.v2().next()).toMap();
						try {
							return new Message(c.v1(), (String) null, m);
						} catch (NullPointerException e) {
							for (Object k : m.keySet())
								if (null == m.get(k)) {
									logger.error("Mongo result field [" + k + "] null at table [" + c.v1() + "], id [" + m.get("_id")
											+ "].");
									m.remove(k);
								}
							return new Message(c.v1(), (String) null, m);
						}
					}
				} catch (MongoException ex) {
					logger.warn("Mongo fail fetch, ignore and continue retry...");
				} catch (IllegalStateException ex) {
					break;
				} finally {
					if (null != c && !cursors.offer(c)) logger.error("MongoDB cursor [" + c.v1() + "] lost.");
				}
		return null;
	}

	public MongoInput batch(int batching) {
		for (Pair<String, DBCursor> c : cursors)
			c.v2().batchSize(batching);
		return this;
	}
}
