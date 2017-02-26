package net.butfly.albatis.mongodb;

import static net.butfly.albatis.mongodb.MongoConnection.dbobj;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Joiner;
import com.mongodb.BasicDBList;
import com.mongodb.Bytes;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import net.butfly.albacore.io.InputImpl;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.logger.Logger;

public class MongoInput extends InputImpl<DBObject> {
	private static final Logger logger = Logger.getLogger(MongoInput.class);
	private final MongoConnection conn;
	private DBCursor cursor;
	private final ReentrantReadWriteLock lock;

	public MongoInput(final String name, String uri, final String table, int inputBatchSize, final DBObject... filter) throws IOException {
		super(name);
		lock = new ReentrantReadWriteLock();
		logger.info("[" + name + "] from [" + uri + "], core [" + table + "]");
		URISpec spec = new URISpec(uri);
		conn = new MongoConnection(spec);
		logger.debug("[" + name + "] find begin...");
		open(() -> {
			cursor = open(spec, table, filter);
			cursor = cursor.batchSize(inputBatchSize).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
		});
	}

	private DBCursor open(URISpec spec, String table, DBObject[] filter) {
		DBCursor cursor;
		long now;
		if (null == filter || filter.length == 0) {
			now = System.nanoTime();
			cursor = conn.collection(table).find();
		} else {
			logger.info("[" + name + "] filters: \n\t" + Joiner.on("\n\t").join(filter) + "\nnow count:");
			if (filter.length == 1) {
				now = System.nanoTime();
				cursor = conn.collection(table).find(filter[0]);
			} else {
				BasicDBList filters = new BasicDBList();
				for (DBObject f : filter)
					filters.add(f);
				now = System.nanoTime();
				cursor = conn.collection(table).find(dbobj("$and", filters));
			}
		}
		String p = spec.getParameter("limit");
		if (p != null) cursor.limit(Integer.parseInt(p));
		p = spec.getParameter("skip");
		if (p != null) cursor.skip(Integer.parseInt(p));
		int count = cursor.count();
		logger.debug(() -> "[" + name + "] find [" + count + " records], end in [" + (System.nanoTime() - now) / 1000 + " ms].");
		logger.trace(() -> "[" + name + "] find [" + cursor.size() + " records].");
		return cursor;
	}

	@Override
	public void close() {
		super.close(this::closeMongo);
	}

	private void closeMongo() {
		cursor.close();
		conn.close();
	}

	@Override
	public boolean empty() {
		if (lock.writeLock().tryLock()) try {
			return !cursor.hasNext();
		} catch (MongoException ex) {
			logger.warn("[" + name() + "] check failure but processing will continue", ex);
			return false;
		} finally {
			lock.writeLock().unlock();
		}
		else return false;
	}

	@Override
	protected DBObject dequeue() {
		do {
			if (lock.writeLock().tryLock()) try {
				return cursor.hasNext() ? cursor.next() : null;
			} catch (MongoException ex) {
				return null;
			} finally {
				lock.writeLock().unlock();
			}
		} while (true);
	}
}
