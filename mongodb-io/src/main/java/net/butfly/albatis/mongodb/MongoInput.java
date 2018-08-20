package net.butfly.albatis.mongodb;

import static net.butfly.albatis.mongodb.MongoConnection.dbobj;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;

public class MongoInput extends net.butfly.albacore.base.Namedly implements OddInput<Rmap> {
	private static final long serialVersionUID = -1542477278520256900L;
	private static final Logger logger = Logger.getLogger(MongoInput.class);
	private final MongoConnection conn;
	private final BlockingQueue<Cursor> cursors = new LinkedBlockingQueue<>();
	private final Map<String, Cursor> cursorsMap = Maps.of();

	public MongoInput(String name, MongoConnection conn) throws IOException {
		super(name);
		this.conn = conn;
		closing(this::closeMongo);
	}

	@Override
	public void open() {
		OddInput.super.open();
		if (cursors.isEmpty()) {
			if (null != conn.defaultCollection()) table(conn.defaultCollection());
			else throw new RuntimeException("No table defined for input.");
		}
	}

	@Override
	public Statistic trace() {
		return new Statistic(this).<DBObject> sizing(b -> (long) b.keySet().size()) //
				.<DBObject> sampling(DBObject::toString).detailing(() -> "[Stats Field Count, not Bytes]");
	}

	public void table(String... tables) {
		Map<String, DBObject> queries = Maps.of();
		for (String t : tables)
			queries.put(t, dbobj());
		if (!queries.isEmpty()) table(queries);
	}

	public void table(String table, DBObject query) {
		table(Maps.of(table, query));
	}

	public void table(Map<String, DBObject> tablesAndQueries) {
		if (null == tablesAndQueries || tablesAndQueries.isEmpty()) return;
		logger.info("[" + name + "] from [" + conn.uri().toString() + "], collection [" + tablesAndQueries.toString() + "]");
		List<Runnable> queries = Colls.list();
		for (String t : tablesAndQueries.keySet())
			queries.add(() -> cursorsMap.computeIfAbsent(t, tt -> new Cursor(tt, tablesAndQueries.get(t))));
		Exeter.of().join(queries.toArray(new Runnable[queries.size()]));
	}

	private class Cursor {
		final String col;
		final DBCursor cursor;

		public Cursor(String col, DBObject q) {
			super();
			this.col = col;
			DBCursor c;
			try {
				c = conn.cursor(col, q).batchSize(conn.getBatchSize()).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
				if (c.hasNext()) {
					c.batchSize(batchSize());
					logger.info("MongoDB query [" + col + "] successed, count: [" + c.count() + "].");
					cursors.add(this);
				} else {
					logger.warn("MongoDB query [" + col + "] finished but empty.");
					c = null;
				}
			} catch (Exception ex) {
				logger.error("MongoDB query [" + col + "] failed", ex);
				c = null;
			}
			cursor = c;
		}

		public void close() {
			try {
				cursor.close();
			} finally {
				cursorsMap.remove(col);
			}
		}

	}

	private void closeMongo() {
		Cursor c;
		while (!cursorsMap.isEmpty())
			if (null != (c = cursors.poll())) c.close();
		conn.close();
	}

	@Override
	public boolean empty() {
		return cursorsMap.isEmpty();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Rmap dequeue() {
		Cursor c;
		Map<String, Object> m;
		while (opened() && !empty())
			if (null != (c = cursors.poll())) try {
				if (c.cursor.hasNext()) {
					try {
						m = s().stats(c.cursor.next()).toMap();
					} catch (MongoException ex) {
						logger.warn("Mongo fail fetch, ignore and continue retry...");
						continue;
					} catch (IllegalStateException ex) {
						continue;
					}
					String collection = c.col;
					Object key = m.containsKey("_id") ? m.get("_id").toString() : null;
					Rmap msg = new Rmap(collection, key);
					m.forEach((k, v) -> {
						if (null == v) {
							logger.error("Mongo result field [" + k + "] null at table [" + collection + "], id [" + key + "].");
						} else {
							msg.put(k, v);
						}
					});
					return msg;
				} else {
					c.close();
					c = null;
				}
			} finally {
				if (null != c) cursors.offer(c);
			}

		return null;
	}

	@Override
	public Input<Rmap> filter(Map<String, Object> criteria) {
		Collection<Cursor> exists = cursorsMap.values();
		for (Cursor c : exists) {
			c.close();
			cursorsMap.put(c.col, new Cursor(c.col, new BasicDBObject(criteria)));
		}
		return this;
	}
}
