package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Joiner;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import net.butfly.albacore.io.Input;
import net.butfly.albacore.utils.logger.Logger;

public class MongoInput extends Input<DBObject> {
	private static final long serialVersionUID = -142784733788535129L;
	private static final Logger logger = Logger.getLogger(MongoInput.class);
	private final MongoConnection conn;
	private DBCursor cursor;
	private final ReentrantReadWriteLock lock;

	public MongoInput(final String name, String uri, final String table, int inputBatchSize, final DBObject... filter) throws IOException {
		super(name);
		lock = new ReentrantReadWriteLock();
		logger.info("[" + name + "] from [" + uri + "], core [" + table + "]");
		this.conn = new MongoConnection(uri);
		long now;
		logger.debug("[" + name + "] find begin...");
		if (null == filter || filter.length == 0) {
			now = System.nanoTime();
			cursor = conn.db().getCollection(table).find();
		} else {
			logger.info("[" + name + "] filters: \n\t" + Joiner.on("\n\t").join(filter));
			if (filter.length == 1) {
				now = System.nanoTime();
				cursor = conn.db().getCollection(table).find(filter[0]);
			} else {
				BasicDBList filters = new BasicDBList();
				for (DBObject f : filter)
					filters.add(f);
				DBObject and = new BasicDBObject();
				and.put("$and", filters);
				now = System.nanoTime();
				cursor = conn.db().getCollection(table).find(and);
			}
		}
		int count = cursor.count();
		logger.debug(() -> "[" + name + "] find [" + count + " records], end in [" + (System.nanoTime() - now) / 1000 + " ms].");
		logger.trace(() -> "[" + name + "] find [" + cursor.size() + " records].");
		cursor = cursor.batchSize(inputBatchSize).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
	}

	@Override
	public void close() {
		super.close(() -> {
			cursor.close();
			try {
				conn.close();
			} catch (IOException e) {
				logger().error("Close failure", e);
			}
		});
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
	public long size() {
		return super.size();
	}

	@Override
	public DBObject dequeue0() {
		if (lock.writeLock().tryLock()) try {
			return cursor.hasNext() ? cursor.next() : null;
		} catch (MongoException ex) {
			logger.warn("[" + name() + "] read failure but processing will continue", ex);
			return null;
		} finally {
			lock.writeLock().unlock();
		}
		else return null;
	}

	@Override
	public List<DBObject> dequeue(long batchSize) {
		boolean retry;
		List<DBObject> batch = new ArrayList<>();
		do {
			retry = false;
			if (lock.writeLock().tryLock()) try {
				while (opened() && batch.size() < batchSize) {
					if (!cursor.hasNext()) return batch;
					batch.add(cursor.next());
				}
			} catch (MongoException ex) {
				logger.warn("[" + name() + "] read failure but processing will continue", ex);
				retry = true;
			} finally {
				lock.writeLock().unlock();
			}
		} while (opened() && retry && batch.size() == 0);
		return batch;
	}

	public final void limit(int limit) {
		cursor = cursor.limit(limit);
	}

	public final void skip(int skip) {
		cursor = cursor.skip(skip);
	}
}
