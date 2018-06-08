package com.hzcominfo.dataggr.spark.integrate.mongo;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class MongoStreamWriter extends ForeachWriter<Row> implements Serializable {
	private static final long serialVersionUID = 6158741366875874741L;
	DBCollection coll;
	Map<String, String> options;
	MongoClient mongoClient;
	public MongoStreamWriter(Map<String, String> options) {
		this.options = options;
	}
	
	@Override
	public void close(Throwable arg0) {
		mongoClient.close();
	}

	@Override
	public boolean open(long arg0, long arg1) {
		mongoClient = new MongoClient(new MongoClientURI(options.get("uri")));
		@SuppressWarnings("deprecation")
		DB db = mongoClient.getDB(options.get("database"));
		coll = db.getCollection(options.get("collection"));
		return true;
	}

	@Override
	public void process(Row row) {
		StructType schema = row.schema();
		String[] fieldNames = schema.fieldNames();
		DBObject obj = new BasicDBObject();
		for (String fn : fieldNames)
			obj.put(fn, row.getAs(fn));
		coll.insert(obj);
	}
}
