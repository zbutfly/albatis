package com.hzcominfo.dataggr.spark.integrate;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;

public class Client implements AutoCloseable, Serializable {
	private static final long serialVersionUID = 5093686615279489589L;
	private SparkSession spark;
	static Map<String, String> defaultConfMap = new HashMap<>();
	// private final static Map<String, Adapter> adapters = new
	// ConcurrentHashMap<>();
	private final URISpec uriSpec;
	private final Adapter adapter;
	private static String master = "local[*]";
	private String appName = "Simulation";

	static {
		defaultConfMap.put("spark.sql.shuffle.partitions", "2001");
		defaultConfMap.put("spark.mongodb.input.uri", "mongodb://user:pwd@localhost:80/db.tbl");
	}

	public Client(String name, URISpec uriSpec) {
		this.uriSpec = uriSpec;
		this.adapter = adapt(uriSpec);
		SparkConf sparkConf = new SparkConf();
		defaultConfMap.forEach((key, value) -> sparkConf.set(key, value));
		this.spark = SparkSession.builder().master(master).appName(name == null ? appName : name).config(sparkConf)
				.getOrCreate();
	}

	private Adapter adapt(URISpec uriSpec) {
		// return adapters.compute(uriSpec.getScheme(), (u, a) -> null == a ?
		// Adapter.adapt(uriSpec) : a);
		return Adapter.adapt(uriSpec);
	}

	public Dataset<Row> read() {
		return adapter.read(uriSpec, spark);
	}
	
	public Dataset<Row> read(URISpec uriSpec) {
		Adapter adapter = adapt(uriSpec);
		return adapter.read(uriSpec, spark);
	}

	@Override
	public void close() {
		if (spark != null) {
			spark.cloneSession();
			spark.close();
		}
	}
}
