package com.hzcominfo.dataggr.spark.integrate.kafka;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.hzcominfo.dataggr.spark.util.BytesUtils;
import com.hzcominfo.dataggr.spark.util.FuncUtil;

import net.butfly.albacore.io.URISpec;

public class SparkKafkaEtlInput extends SparkKafkaInput {
	private static final long serialVersionUID = -8077483839198954L;
	private StructType innerSchema = null;

	public SparkKafkaEtlInput() {
		super();
	}

	public SparkKafkaEtlInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
	}

	@Override
	protected String schema() {
		return "kafka:etl";
	}

	@Override
	public Dataset<Row> read() {
		if (null == innerSchema) innerSchema = new StructType(new StructField[] { //
				DataTypes.createStructField("oper_type", DataTypes.StringType, true) });
		return super.read().map(this::etl, RowEncoder.apply(innerSchema));
	}

	public Row etl(Row r) {
		byte[] bytes = r.getAs("value");
		Map<String, Object> der = BytesUtils.der(bytes);
		@SuppressWarnings("unchecked")
		Map<String, Object> value = (Map<String, Object>) der.get("value");
		Row row = FuncUtil.mapRow(value);
		StructType sch = row.schema();
		innerSchema = innerSchema.merge(sch);
		der.get("oper_type");
		return row;
	}
}
