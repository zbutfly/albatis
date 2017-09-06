package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.mongodb.Bytes;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.OddInput;

public class MongoSimpleInput extends OddInput<Message> {
	private static final Logger logger = Logger.getLogger(MongoSimpleInput.class);
	private final MongoConnection conn;
	private DBCursor cursor;
	private final ReentrantReadWriteLock lock;
	private String table;

	public MongoSimpleInput(final String name, MongoConnection conn, String table, DBObject... filter) throws IOException {
		super(name);
		lock = new ReentrantReadWriteLock();
		this.table = table;
		logger.info("[" + name + "] from [" + conn.toString() + "], collection [" + table + "]");
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

	@SuppressWarnings("unchecked")
	@Override
	protected Message dequeue() {
		boolean hasNext = true;
		do {
			if (lock.writeLock().tryLock()) try {
				if (!(hasNext = cursor.hasNext())) return null;
				else return new Message(table, (String) null, cursor.next().toMap());
			} catch (MongoException ex) {
				logger.warn("Mongo fail fetch, ignore and continue retry...");
			} catch (IllegalStateException ex) {
				break;
			} finally {
				lock.writeLock().unlock();
			}
		} while (!hasNext);
		return null;
	}

	public MongoSimpleInput batch(int batching) {
		cursor = cursor.batchSize(batching);
		return this;
	}
}
