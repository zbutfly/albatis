package net.butfly.albatis.mongodb;

import static net.butfly.albacore.utils.parallel.Parals.eachs;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.mongodb.Bytes;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
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
		eachs(Stream.of(tables), t -> {
			try {
				cursors.put(new Pair<>(t, conn.cursor(t).batchSize(conn.getBatchSize()).addOption(Bytes.QUERYOPTION_NOTIMEOUT)));
			} catch (Exception e) {
				logger.error("MongoDB query [" + t + "]failed", e);
			} finally {
				queryings.decrementAndGet();
			}
		});
		closing(this::closeMongo);
	}

	public MongoInput(String name, MongoConnection conn, Map<String, DBObject> tablesAndQueries) throws IOException {
		super(name);
		if (null == tablesAndQueries) {
			if (conn.defaultCollection() == null) throw new IOException("MongoDB could not input whith non or null table.");
			tablesAndQueries = new ConcurrentHashMap<>();
			tablesAndQueries.put(conn.defaultCollection(), MongoConnection.dbobj());
		}
		logger.info("[" + name + "] from [" + conn.toString() + "], collection [" + tablesAndQueries.toString() + "]");
		queryings = new AtomicInteger(tablesAndQueries.size());
		this.conn = conn;
		logger.debug("[" + name + "] find begin...");
		cursors = new LinkedBlockingQueue<>(tablesAndQueries.size());
		eachs(tablesAndQueries.entrySet(), e -> {
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
		});
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
