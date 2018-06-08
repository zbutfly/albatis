package com.hzcominfo.dataggr.spark.integrate.kafka;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.util.BytesUtils;
import com.hzcominfo.dataggr.spark.util.FuncUtil;

import net.butfly.albacore.io.URISpec;

public class SparkKafkaEtlInput extends SparkKafkaInput {
	private static final long serialVersionUID = -8077483839198954L;
	final static String schema = "kafka:etl";
	private Encoder<Row> encoder;

	public SparkKafkaEtlInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
	}

	@Override
	public Dataset<Row> load() {
		return super.load().map(this::etl, encoder);
	}

	protected Row etl(Row r) {
		byte[] bytes = r.getAs("value");
		Map<String, Object> der = BytesUtils.der(bytes);
		@SuppressWarnings("unchecked")
		Map<String, Object> value = (Map<String, Object>) der.get("value");
		der.get("oper_type");
		return FuncUtil.mapRow(value);
	}
}
