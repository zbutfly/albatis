package net.butfly.albatis.mongodb;

import java.io.IOException;
import java.util.Map;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import net.butfly.albacore.io.InputQueueImpl;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.mongodb.Mongos.MongoDB;

public class MongodbInput extends InputQueueImpl<DBObject> {
	private static final long serialVersionUID = -142784733788535129L;
	private static final Logger logger = Logger.getLogger(MongodbInput.class);
	private final MongoDB mdb;
	private DBCursor cursor;

	public MongodbInput(final String name, String uri, final String table, int inputBatchSize, final String... filter) throws IOException {
		super(name);
		logger.info("MongoDBInput [" + name + "] from [" + uri + "], core [" + table + "]");
		this.mdb = new MongoDB(uri);
		if (null == filter || filter.length == 0) cursor = mdb.connect().getCollection(table).find();
		else if (filter.length == 1) {
			Map<String, Object> m = JsonSerder.JSON_MAPPER.der(filter[0]);
			cursor = mdb.connect().getCollection(table).find(new BasicDBObject(m));
		} else {
			BasicDBList filters = new BasicDBList();
			for (int i = 0; i < filter.length; i++) {
				Map<String, Object> m = JsonSerder.JSON_MAPPER.der(filter[i]);
				filters.add(new BasicDBObject(m));
			}
			DBObject and = new BasicDBObject();
			and.put("$and", filters);
			cursor = mdb.connect().getCollection(table).find(and);
		}
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
		return !cursor.hasNext();
	}

	@Override
	public long size() {
		return super.size();
	}

	@Override
	protected DBObject dequeueRaw() {
		synchronized (cursor) {
			return cursor.hasNext() ? cursor.next() : null;
		}
	}

	public final void limit(int limit) {
		cursor = cursor.limit(limit);
	}

	public final void skip(int skip) {
		cursor = cursor.skip(skip);
	}
}
