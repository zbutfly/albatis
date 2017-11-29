package net.butfly.albatis.mongodb;

import static net.butfly.albacore.paral.Sdream.of;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.mongodb.Bytes;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.parallel.Lambdas;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.OddInput;

public class MongoInput extends net.butfly.albacore.base.Namedly implements OddInput<Message> {
	private static final Logger logger = Logger.getLogger(MongoInput.class);
	private final MongoConnection conn;
	private final BlockingQueue<Cursor> cursors = new LinkedBlockingQueue<>();
	private final Map<String, Cursor> cursorsMap = Maps.of();

	public MongoInput(String name, MongoConnection conn) throws IOException {
		super(name);
		if (null == tables) {
			if (conn.defaultCollection() == null) throw new IOException("MongoDB could not input whith non or null table.");
			tables = new String[] { conn.defaultCollection() };
		}
		logger.info("[" + name + "] from [" + conn.toString() + "], collection [" + Joiner.on(",").join(tables) + "]");
		queryings = new AtomicInteger(tables.length);
		this.conn = conn;
		logger.debug("[" + name + "] find begin...");
		cursors = new LinkedBlockingQueue<>(tables.length);
		List<Pair<String, DBCursor>> ss = of(tables).join(new Function<String, DBCursor>() {
			@Override
			public DBCursor apply(String t) {
				try {
					return conn.cursor(t).batchSize(conn.getBatchSize()).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
				} catch (Exception e) {
					logger.error("MongoDB query [" + t + "]failed", e);
					return null;
				} finally {
					queryings.decrementAndGet();
				}
			}
		}).filter(Lambdas.notNull()).list();
		cursors.addAll(ss);
		closing(this::closeMongo);
	}

	public MongoInput(String name, MongoConnection conn, Map<String, DBObject> tablesAndQueries) throws IOException {
		super(name);
		if (null == tablesAndQueries) {
			if (conn.defaultCollection() == null) throw new IOException("MongoDB could not input whith non or null table.");
			tablesAndQueries = Maps.of();
			tablesAndQueries.put(conn.defaultCollection(), MongoConnection.dbobj());
		}
		logger.info("[" + name + "] from [" + conn.toString() + "], collection [" + tablesAndQueries.toString() + "]");
		queryings = new AtomicInteger(tablesAndQueries.size());
		this.conn = conn;
		logger.debug("[" + name + "] find begin...");
		cursors = new LinkedBlockingQueue<>(tablesAndQueries.size());
		cursors.addAll(of(tablesAndQueries).map(e -> {
			try {
				return new Pair<>(e.getKey(), conn.cursor(e.getKey(), e.getValue()).batchSize(conn.getBatchSize()).addOption(
						Bytes.QUERYOPTION_NOTIMEOUT));
			} catch (Exception ex) {
				logger.error("MongoDB query [" + e.getKey() + "]failed", ex);
				return null;
			} finally {
				cursorsMap.remove(col);
			}
		}).list());
		closing(this::closeMongo);
		open();
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
	public Message dequeue() {
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
					Message msg = new Message(collection, key);
					m.forEach((k, v) -> {
						if (null == v) {
							logger.error("Mongo result field [" + k + "] null at table [" + collection + "], id [" + key + "].");
						} else { msg.put(k, v); }
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
}
