package com.hzcominfo.dataggr.spark.integrate.kafka;

import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import com.hzcominfo.dataggr.spark.util.BytesUtils;
import com.hzcominfo.dataggr.spark.util.FuncUtil;
import com.hzcominfo.dataggr.spark.util.RowConsumer;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.logger.Logger;

public class SparkKafkaEtlInput extends SparkKafkaInput {
	private static final long serialVersionUID = -8077483839198954L;
	private static final Logger logger = Logger.getLogger(SparkKafkaEtlInput.class);

	// private Accumulable<StructType, StructType> innerSchema = null;

	public SparkKafkaEtlInput() {
		super();
	}

	public SparkKafkaEtlInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
		// this.jsc.broadcast(sch);
	}

	@Override
	protected String schema() {
		return "kafka:etl";
	}

	@Override
	public StreamingQuery read(RowConsumer using) {
		// if (null == innerSchema) innerSchema = jsc.accumulable(sch, "innerSchema", acc);
		return super.read(using.before(this::etl));
		// Dataset<Row> ds = super.read();
		// ds.writeStream().start();
		// Row r0 = ds.first();
		// // ds.union(spark.createDataset(Colls.list(r0), RowEncoder.apply(ds.schema())));
		// return ds.map(this::etl, RowEncoder.apply(etl(r0).schema()));
	}

	public Row etl(Row r) {
		byte[] bytes = r.getAs("value");
		Map<String, Object> der = BytesUtils.der(bytes);
		@SuppressWarnings("unchecked")
		Map<String, Object> value = (Map<String, Object>) der.get("value");
		logger.trace("etl exacted: " + value.toString());
		Row row = FuncUtil.mapRow(value);
		der.get("oper_type");
		return row;
	}

	// private static AccumulableParam<StructType, StructType> acc = new AccumulableParam<StructType, StructType>() {
	// private static final long serialVersionUID = -6918366806713193273L;
	//
	// @Override
	// public StructType addAccumulator(StructType v1, StructType v2) {
	// return v1.merge(v2);
	// }
	//
	// @Override
	// public StructType addInPlace(StructType v1, StructType v2) {
	// return v1.merge(v2);
	// }
	//
	// @Override
	// public StructType zero(StructType v1) {
	// return v1;
	// }
	// };
	// private static StructType sch = new StructType(new StructField[] { //
	// DataTypes.createStructField("oper_type", DataTypes.StringType, true) });
}
