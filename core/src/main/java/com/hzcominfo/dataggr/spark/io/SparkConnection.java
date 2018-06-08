package com.hzcominfo.dataggr.spark.io;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.albatis.nosql.Connection;

import net.butfly.albacore.io.URISpec;

public class SparkConnection implements Connection, Serializable {
	private static final long serialVersionUID = 5093686615279489589L;
	private final static String DEFAULT_HOST = "local[*]";

	public final SparkSession spark;
	private final URISpec uriSpec;
	private final Map<String, String> parameters = new ConcurrentHashMap<>();

	public SparkConnection(String name, URISpec uriSpec) {
		this.uriSpec = uriSpec;
		SparkConf sparkConf = new SparkConf();
		parameters.put("spark.sql.shuffle.partitions", "2001");
		parameters.put("spark.mongodb.input.uri", "mongodb://user:pwd@localhost:80/db.tbl");
		parameters.put("spark.mongodb.output.uri", "mongodb://user:pwd@localhost:80/db.tbl");
		parameters.forEach((key, value) -> sparkConf.set(key, value));
		this.spark = SparkSession.builder().master(DEFAULT_HOST).appName(name == null ? "Simulation" : name).config(sparkConf)
				.getOrCreate();
	}

	@Override
	public void close() {
		if (spark != null) {
			spark.cloneSession();
			spark.close();
		}
	}

	@Override
	@Deprecated
	public SparkInput input(String... table) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	@Deprecated
	public SparkOutput output() throws IOException {
		throw new UnsupportedOperationException();
	}

	public <O extends SparkOutput> O output(URISpec uri) {
		return SparkIO.output(spark, uri);
	}

	public <I extends SparkInput> I input(URISpec uri,  String... fields) {
		return SparkIO.input(spark, uri, fields);
	}

	@Override
	public String defaultSchema() {
		return "spark";
	}

	@Override
	public URISpec uri() {
		return uriSpec;
	}

	public <T> Dataset<T> toDS(List<T> rows, Class<T> clazz) {
		return spark.createDataset(rows, Encoders.bean(clazz));
	}
}
