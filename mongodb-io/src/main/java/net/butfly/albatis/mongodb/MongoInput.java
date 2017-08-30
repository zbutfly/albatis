package net.butfly.albatis.mongodb;

import static net.butfly.albatis.mongodb.MongoConnection.dbobj;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Joiner;
import com.hzcominfo.albatis.nosql.Connection;
import com.mongodb.BasicDBList;
import com.mongodb.Bytes;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.InputOddImpl;

public class MongoInput extends InputOddImpl<DBObject> {
	private static final Logger logger = Logger.getLogger(MongoInput.class);
	private final MongoConnection conn;
	private DBCursor cursor;
	private final ReentrantReadWriteLock lock;

	public MongoInput(final String name, URISpec uri, final String table, final DBObject... filter) throws IOException {
		super(name);
		lock = new ReentrantReadWriteLock();
		logger.info("[" + name + "] from [" + uri + "], core [" + table + "]");
		conn = new MongoConnection(uri);
		logger.debug("[" + name + "] find begin...");
		opening(() -> {
			cursor = open(uri, table, filter);
			String bstr;
			if (null != (bstr = uri.getParameter(Connection.PARAM_KEY_BATCH))) cursor = cursor.batchSize(Integer.parseInt(bstr));
			cursor = cursor.addOption(Bytes.QUERYOPTION_NOTIMEOUT);
		});
		closing(this::closeMongo);
		open();
	}

	public MongoInput(final String name, String uri, final String table, final DBObject... filter) throws IOException {
		this(name, new URISpec(uri), table, filter);
	}

	private DBCursor open(URISpec spec, String table, DBObject[] filter) {
		DBCursor cursor;
		if (!conn.collectionExists(table)) throw new IllegalArgumentException("Collection [" + table + "] not existed for input");
		DBCollection col = conn.collection(table);
		long now;
		if (null == filter || filter.length == 0) {
			now = System.nanoTime();
			cursor = col.find();
		} else {
			logger.info("[" + name + "] filters: \n\t" + Joiner.on("\n\t").join(filter) + "\nnow count:");
			if (filter.length == 1) {
				now = System.nanoTime();
				cursor = col.find(filter[0]);
			} else {
				BasicDBList filters = new BasicDBList();
				for (DBObject f : filter)
					filters.add(f);
				now = System.nanoTime();
				cursor = col.find(dbobj("$and", filters));
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
		} catch (IllegalStateException ex) {
			return true;
		} finally {
			lock.writeLock().unlock();
		}
		else return false;
	}

	@Override
	protected DBObject dequeue() {
		DBObject r = null;
		do {
			if (lock.writeLock().tryLock()) try {
				return cursor.hasNext() ? cursor.next() : null;
			} catch (MongoException ex) {
				logger.warn("Mongo fail fetch, ignore and continue retry...");
			} catch (IllegalStateException ex) {
				break;
			} finally {
				lock.writeLock().unlock();
			}
		} while (r == null);
		return r;
	}

	public MongoInput batch(int batching) {
		cursor = cursor.batchSize(batching);
		return this;
	}
}
