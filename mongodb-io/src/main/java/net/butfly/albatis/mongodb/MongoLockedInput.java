package net.butfly.albatis.mongodb;

import static net.butfly.albacore.paral.Sdream.of;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import com.google.common.base.Joiner;
import com.mongodb.Bytes;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.parallel.Lambdas;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.OddInput;

public class MongoLockedInput extends net.butfly.albacore.base.Namedly implements OddInput<Message> {
	private final MongoConnection conn;
	private final List<C> cursors;
	private final AtomicInteger queryings;

	public MongoLockedInput(String name, MongoConnection conn) throws IOException {
		this(name, conn, conn.defaultCollection());
	}

	private class C implements AutoCloseable {
		final String table;
		final DBCursor cursor;
		final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

		public C(String table, DBCursor cursor) {
			super();
			this.table = table;
			this.cursor = cursor;
		}

		@Override
		public void close() {
			try {
				cursor.close();
			} catch (Exception e) {
				logger().error("MongoDB cursor [" + table + "] close fail", e);
			}
		}
	}

	public MongoLockedInput(String name, MongoConnection conn, String... tables) throws IOException {
		super(name);
		if (null == tables) {
			if (conn.defaultCollection() == null) throw new IOException("MongoDB could not input whith non or null table.");
			tables = new String[] { conn.defaultCollection() };
		}
		logger().info("[" + name + "] from [" + conn.toString() + "], collection [" + Joiner.on(",").join(tables) + "]");
		queryings = new AtomicInteger(tables.length);
		this.conn = conn;
		logger().debug("[" + name + "] find begin...");
		cursors = of(tables).map(new Function<String, C>() {
			@Override
			public C apply(String t) {
				try {
					DBCursor c = conn.cursor(t).batchSize(conn.getBatchSize()).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
					return new C(t, c);
				} catch (Exception e) {
					logger().error("MongoDB query [" + t + "]failed", e);
					return null;
				} finally {
					queryings.decrementAndGet();
				}
			}
		}).filter(Lambdas.notNull()).list();
		closing(this::closeMongo);
	}

	public MongoLockedInput(String name, MongoConnection conn, Map<String, DBObject> tablesAndQueries) throws IOException {
		super(name);
		if (null == tablesAndQueries) {
			if (conn.defaultCollection() == null) throw new IOException("MongoDB could not input whith non or null table.");
			tablesAndQueries = Maps.of();
			tablesAndQueries.put(conn.defaultCollection(), MongoConnection.dbobj());
		}
		logger().info("[" + name + "] from [" + conn.toString() + "], collection [" + tablesAndQueries.toString() + "]");
		queryings = new AtomicInteger(tablesAndQueries.size());
		this.conn = conn;
		logger().debug("[" + name + "] find begin...");
		cursors = of(tablesAndQueries).map(e -> {
			try {
				return new C(e.getKey(), conn.cursor(e.getKey(), e.getValue()).batchSize(conn.getBatchSize()).addOption(
						Bytes.QUERYOPTION_NOTIMEOUT));
			} catch (Exception ex) {
				logger().error("MongoDB query [" + e.getKey() + "]failed", ex);
				return null;
			} finally {
				queryings.decrementAndGet();
			}
		}).list();
		closing(this::closeMongo);
	}

	private void closeMongo() {
		while (!cursors.isEmpty())
			try {
				cursors.remove(0).close();
			} catch (Exception e) {}
		conn.close();
	}

	@Override
	public boolean empty() {
		return queryings.get() == 0 && cursors.isEmpty();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Message dequeue() {
		int i = 0;
		while (!cursors.isEmpty())
			try {
				if (i >= cursors.size()) i -= cursors.size();
				C c = cursors.get(i);
				DBObject dbo = null;
				if (!c.lock.writeLock().tryLock()) continue;
				try {
					if (c.cursor.hasNext()) dbo = c.cursor.next();
					else {
						cursors.remove(c);
						c.close();
					}
				} catch (Exception e) {
					logger().warn("Mongodb fail and ignore: " + e.toString());
				} finally {
					c.lock.writeLock().unlock();
				}
				if (null != dbo) return new Message(c.table, (String) null, dbo.toMap());
			} finally {
				i++;
			}
		return null;
	}

	public MongoLockedInput batch(int batching) {
		for (C c : cursors)
			c.cursor.batchSize(batching);
		return this;
	}
}
