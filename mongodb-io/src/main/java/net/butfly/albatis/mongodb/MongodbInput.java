package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import net.butfly.albacore.io.Input;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.mongodb.Mongos.MongoDB;

public class MongodbInput extends Input<DBObject> {
	private static final long serialVersionUID = -142784733788535129L;
	private static final Logger logger = Logger.getLogger(MongodbInput.class);
	private final MongoDB mdb;
	private DBCursor cursor;
	private final ReentrantLock lock;

	public MongodbInput(final String name, String uri, final String table, int inputBatchSize, final String... filter) throws IOException {
		super(name);
		lock = new ReentrantLock();
		logger.info("MongoDBInput [" + name + "] from [" + uri + "], core [" + table + "]");
		this.mdb = new MongoDB(uri);
		long now;
		logger.debug("MongoDBInput [" + name + "] find begin...");
		if (null == filter || filter.length == 0) {
			now = System.nanoTime();
			cursor = mdb.connect().getCollection(table).find();
		} else if (filter.length == 1) {
			Map<String, Object> m = JsonSerder.JSON_MAPPER.der(filter[0]);

			now = System.nanoTime();
			cursor = mdb.connect().getCollection(table).find(new BasicDBObject(m));
		} else {
			BasicDBList filters = new BasicDBList();
			for (int i = 0; i < filter.length; i++) {
				Map<String, Object> m = JsonSerder.JSON_MAPPER.der(filter[i]);
				filters.add(new BasicDBObject(m));
			}
			DBObject and = new BasicDBObject();
			and.put("$and", filters);

			now = System.nanoTime();
			cursor = mdb.connect().getCollection(table).find(and);
		}
		logger.info(() -> "MongoDBInput [" + name + "] find end in [" + (System.nanoTime() - now) / 1000 + " ms].");
		logger.trace(() -> "MongoDBInput [" + name + "] find [" + cursor.size() + " records].");
		cursor = cursor.batchSize(inputBatchSize).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
	}

	@Override
	public void close() {
		logger.debug("MongoDBInput [" + name() + "] closing...");
		cursor.close();
		mdb.close();
		logger.info("MongoDBInput [" + name() + "] closed.");
	}

	@Override
	public boolean empty() {
		if (lock.tryLock()) try {
			return !cursor.hasNext();
		} catch (MongoException ex) {
			logger.warn("MongoDBInput [" + name() + "] check failure but processing will continue", ex);
			return false;
		} finally {
			lock.unlock();
		}
		else return false;
	}

	@Override
	public long size() {
		return super.size();
	}

	@Override
	public DBObject dequeue0() {
		lock.lock();
		try {
			return cursor.hasNext() ? cursor.next() : null;
		} catch (MongoException ex) {
			logger.warn("MongoDBInput [" + name() + "] read failure but processing will continue", ex);
			return null;
		} finally {
			lock.unlock();
		}
	}

	public final void limit(int limit) {
		cursor = cursor.limit(limit);
	}

	public final void skip(int skip) {
		cursor = cursor.skip(skip);
	}
}
