package com.hzcominfo.dataggr.spark.integrate;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;

public class Client implements AutoCloseable {
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
		ReadConfig conf = adapter.getConfig(uriSpec);
		if (conf == null)
			return null;
		if (conf.isStream())
			return spark.readStream().format(conf.getFormat()).options(conf.getOptions()).load();
		else
			return spark.read().format(conf.getFormat()).options(conf.getOptions()).load();
	}

	@Override
	public void close() {
		if (spark != null) {
			spark.cloneSession();
			spark.close();
		}
	}
}
