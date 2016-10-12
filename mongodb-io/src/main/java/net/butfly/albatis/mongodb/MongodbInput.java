package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.ParallelScanOptions;

import net.butfly.albacore.io.InputQueueImpl;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.mongodb.Mongos.MDB;

public class MongodbInput extends InputQueueImpl<DBObject, DBObject> {
	private static final long serialVersionUID = -142784733788535129L;
	private static final Logger logger = Logger.getLogger(MongodbInput.class);
	private final boolean continuous;
	private final MDB mdb;
	private final DBCollection collection;
	private final List<Cursor> cursors;

	public MongodbInput(final String table, String configFile, final DBObject... filter) throws IOException {
		this(table, configFile, 1, 1000, false, filter);
	}

	public MongodbInput(final String table, String configFile, int parallelism, int inputBatchSize, boolean continuous,
			final DBObject... filter) throws IOException {
		super("mongodb-input-queue");
		this.continuous = continuous;
		this.mdb = Mongos.mongoConnect(configFile);
		this.collection = mdb.db().getCollection(table);
		if (parallelism > 1) {
			ParallelScanOptions op = ParallelScanOptions.builder().batchSize(inputBatchSize).numCursors(parallelism).build();
			logger.info("Mongodb parallel scan @" + table + ", #" + parallelism + ", $" + inputBatchSize + " created.");
			cursors = this.collection.parallelScan(op);
		} else {
			Cursor c;
			if (null == filter || filter.length == 0) c = this.collection.find();
			else if (filter.length == 1) c = this.collection.find(filter[0]);
			else {
				BasicDBList filters = new BasicDBList();
				for (int i = 0; i < filter.length; i++)
					filters.add(filter[i]);
				DBObject and = new BasicDBObject();
				and.put("$and", filters);
				c = this.collection.find(and);
			}
			this.cursors = Arrays.asList(c);
		}
	}

	@Override
	public void close() {
		for (Cursor c : cursors)
			c.close();
		mdb.close();
	}

	@Override
	public boolean empty() {
		for (Cursor c : cursors)
			if (c.hasNext()) return false;
		return true;
	}

	@Override
	protected DBObject dequeueRaw() {
		for (Cursor c : Collections.disorderize(cursors))// XXX
			if (c.hasNext()) return c.next();
		return null;
	}

	@Override
	public DBObject dequeue() {
		return continuous ? dequeueWait() : super.dequeue();
	}

	@Override
	public List<DBObject> dequeue(long batchSize) {
		return continuous ? dequeueWait(batchSize) : super.dequeue(batchSize);
	}
}
