package com.hzcominfo.dataggr.spark.integrate.kafka;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.util.BytesUtils;

import net.butfly.albacore.io.URISpec;

public class SparkKafkaEtlInput extends SparkKafkaInput {
	private static final long serialVersionUID = -8077483839198954L;
	final static String schema = "kafka:etl";

	protected SparkKafkaEtlInput(SparkSession spark, URISpec targetUri) {
		this(spark, targetUri, new String[] {});
	}

	public SparkKafkaEtlInput(SparkSession spark, URISpec targetUri, String... fields) {
		super(spark, targetUri, fields);
	}

	@Override
	public Dataset<Row> dequeue() {
		return super.dequeue();
	}

	@Override
	protected Row selectItems(Row r) {
		byte[] bytes = r.getAs("value");
		Map<String, Object> der = BytesUtils.der(bytes);
		@SuppressWarnings("unchecked")
		Map<String, Object> value = (Map<String, Object>) der.get("value");
		String[] fields = new String[] {"CERTIFIED_ID", "TRAIN_DAY"};
		Object[] arr = new Object[fields.length];
		for (int i = 0; i < fields.length; i++)
			arr[i] = value.get(fields[i]);
		return RowFactory.create(arr);
	}
}
