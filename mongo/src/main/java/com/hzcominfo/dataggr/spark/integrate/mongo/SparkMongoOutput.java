package com.hzcominfo.dataggr.spark.integrate.mongo;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.hzcominfo.dataggr.spark.io.SparkOutput;
import com.hzcominfo.dataggr.spark.util.FuncUtil;
import com.mongodb.spark.MongoSpark;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.logger.Logger;

public class SparkMongoOutput extends SparkOutput {
	private static final long serialVersionUID = -887072515139730517L;
	private static final Logger logger = Logger.getLogger(SparkMongoOutput.class);

	private static final String writeconcern = "majority";
//	private WriteConfig writeConfig;
	private MongoSpark mongo;
	private String dbName;
	private String collectionName;

	public SparkMongoOutput() {
		super();
	}

	public SparkMongoOutput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
		Map<String, String> opts = options();
//		writeConfig = WriteConfig.create(jsc).withOptions(opts);
		mongo = MongoSpark.builder().javaSparkContext(jsc).options(opts).build();
		// mongoClient = new MongoClient(new MongoClientURI(opts.get("uri")));
		this.dbName = opts.get("database");
		this.collectionName = opts.get("collection");
	}

	@Override
	public void close() {
		super.close();
		// mongoClient.close();
	}

	@Override
	protected Map<String, String> options() {
		String file = targetUri.getFile();
		String[] path = targetUri.getPaths();
		if (path.length != 1) throw new RuntimeException("Mongodb URI is incorrect");
		String database = path[0];
		String collection = file;
		String uri = targetUri.getScheme() + "://" + targetUri.getAuthority() + "/" + database;

		Map<String, String> options = new HashMap<String, String>();
		options.put("uri", uri);
		options.put("database", database);
		options.put("collection", collection);
		options.put("writeConcern.w", writeconcern);
		return options;
	}

	@Override
	public void write(Row row) {
		mongo.connector().mongoClientFactory().create().getDatabase(dbName).getCollection(collectionName).insertOne(new Document(FuncUtil
				.rowMap(row)));
		// Dataset<Row> ds = spark
		// mongo.sparkSession().createDataset(Colls.list(row), RowEncoder.apply(row.schema()));
		// MongoSpark.save(ds, writeConfig);
		logger.info("inserted: " + row.toString());
	}

	@Override
	protected String schema() {
		return "mongodb";
	}
}
