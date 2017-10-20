package net.butfly.albatis.mongodb;

import static net.butfly.albacore.utils.parallel.Parals.eachs;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.google.common.base.Joiner;
import com.mongodb.Bytes;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.OddInput;

public class MongoInput extends OddInput<Message> {
	private static final Logger logger = Logger.getLogger(MongoInput.class);
	private final MongoConnection conn;
	private final BlockingQueue<Pair<String, DBCursor>> cursors;
	private final AtomicInteger queryings;

	public MongoInput(String name, MongoConnection conn) throws IOException {
		this(name, conn, conn.defaultCollection());
	}

	public MongoInput(String name, MongoConnection conn, String... tables) throws IOException {
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
		open();
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
				cursors.put(new Pair<>(e.getKey(), conn.cursor(e.getKey(), e.getValue()).batchSize(conn.getBatchSize()).addOption(
						Bytes.QUERYOPTION_NOTIMEOUT)));
			} catch (Exception ex) {
				logger.error("MongoDB query [" + e.getKey() + "]failed", ex);
			} finally {
				queryings.decrementAndGet();
			}
		});
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
			return null;
		} catch (Exception e) {
			logger.error("MongoDB cursor [" + c.v1() + "] close fail", e);
			return c;
		}
	}

	@Override
	public boolean empty() {
		return queryings.get() == 0 && cursors.isEmpty();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Message dequeue() {
		Pair<String, DBCursor> c;
		while (null != (c = cursors.poll()))
			try {
				if (!c.v2().hasNext()) c = closeCursor(c);
				else return new Message(c.v1(), (String) null, c.v2().next().toMap());
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
