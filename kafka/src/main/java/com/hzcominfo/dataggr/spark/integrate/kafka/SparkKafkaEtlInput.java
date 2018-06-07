package com.hzcominfo.dataggr.spark.integrate.kafka;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.hzcominfo.dataggr.spark.util.BytesUtils;

import net.butfly.albacore.io.URISpec;

public class SparkKafkaEtlInput extends SparkKafkaInput {
	private static final long serialVersionUID = -8077483839198954L;
	private final String[] fields;
	private final StructField[] sfields;
	private final StructType stype;
	final static String schema = "kafka:etl";

	public SparkKafkaEtlInput(SparkSession spark, URISpec targetUri) {
		this(spark, targetUri, System.getProperty("dataggr.feature.kafka.etl.fields", "CERTIFIED_ID,TRAIN_DAY").split(","));
	}

	public SparkKafkaEtlInput(SparkSession spark, URISpec targetUri, String... fields) {
		super(spark, targetUri);
		this.fields = fields;
		sfields = new StructField[fields.length];
		for (int i = 0; i < fields.length; i++)
			sfields[i] = DataTypes.createStructField(fields[i], DataTypes.StringType, true);
		stype = DataTypes.createStructType(sfields);
	}

	@Override
	public Dataset<Row> dequeue() {
		return super.dequeue().map(this::etl, RowEncoder.apply(stype));
	}

	private Row etl(Row r) throws Exception {
		byte[] bytes = r.getAs("value");
		Map<String, Object> der = BytesUtils.der(bytes);
		@SuppressWarnings("unchecked")
		Map<String, Object> value = (Map<String, Object>) der.get("value");
		Object[] arr = new Object[stype.fieldNames().length];
		for (int i = 0; i < fields.length; i++)
			arr[i] = value.get(fields[i]);
		return RowFactory.create(arr);
	}
}
