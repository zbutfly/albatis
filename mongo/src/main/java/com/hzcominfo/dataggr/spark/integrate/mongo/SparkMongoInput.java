package com.hzcominfo.dataggr.spark.integrate.mongo;

import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.io.SparkInput;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

import net.butfly.albacore.io.URISpec;

public class SparkMongoInput extends SparkInput {
	private static final long serialVersionUID = -6246162473887054138L;
	final static String schema = "mongodb";

	protected SparkMongoInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
	}

	@Override
	public Dataset<Row> dequeue() {
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		ReadConfig readConfig = ReadConfig.create(jsc).withOptions(options(targetUri));
		return MongoSpark.load(jsc, readConfig).toDF();
	}

	@Override
	protected Map<String, String> options(URISpec uriSpec) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Row selectItems(Row r) {
		return super.defaultSelectItems(r);
	}
}
