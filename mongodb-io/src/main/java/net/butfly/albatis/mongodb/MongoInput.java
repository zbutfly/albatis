package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.mongodb.Bytes;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.InputOddImpl;

public class MongoInput extends InputOddImpl<DBObject> {
	private static final Logger logger = Logger.getLogger(MongoInput.class);
	private final MongoConnection conn;
	private DBCursor cursor;
	private final ReentrantReadWriteLock lock;

	public MongoInput(final String name, MongoConnection conn, final String table, final DBObject... filter) throws IOException {
		super(name);
		lock = new ReentrantReadWriteLock();
		logger.info("[" + name + "] from [" + conn.toString() + "], core [" + table + "]");
		this.conn = conn;
		logger.debug("[" + name + "] find begin...");
		opening(() -> {
			cursor = conn.cursor(table, filter);
			cursor = cursor.batchSize(conn.getBatchSize());
			cursor = cursor.addOption(Bytes.QUERYOPTION_NOTIMEOUT);
		});
		closing(this::closeMongo);
		open();
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
