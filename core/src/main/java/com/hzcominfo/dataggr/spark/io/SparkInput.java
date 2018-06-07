package com.hzcominfo.dataggr.spark.io;

import java.io.Serializable;
import java.util.function.Consumer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Message;

public abstract class SparkInput extends SparkIO implements Input<Message>, Serializable {
	private static final long serialVersionUID = 6966901980613011951L;
	private final Dataset<Row> dataset;
	protected String[] fields;
	protected StructType stype;

	protected SparkInput(SparkSession spark, URISpec targetUri, String... fields) {
		super(spark, targetUri);
		if (fields == null || fields.length < 1)
			dataset = dequeue();
		else {
			this.fields = fields;
			StructField[] sfields = new StructField[fields.length];
			for (int i = 0; i < fields.length; i++)
				sfields[i] = DataTypes.createStructField(fields[i], DataTypes.StringType, true);
			stype = DataTypes.createStructType(sfields);
			dataset = dequeue().map(this::selectItems, RowEncoder.apply(stype));
		}
	}

	@Override
	public void close() {
		Input.super.close();
		spark.close();
	}

	public abstract Dataset<Row> dequeue();

	@Override
	public final void dequeue(Consumer<Sdream<Message>> using) {
		using.accept(conv(dequeue()));
	}

	private Sdream<Message> conv(Dataset<Row> dequeue) {
		// TODO Auto-generated method stub
		return null;
	}

	public StreamingQuery start(ForeachWriter<Row> writer) {
		return dataset.writeStream().foreach(writer).start();
	}

	protected abstract Row selectItems(Row r);
	
	protected Row defaultSelectItems(Row r) {
		Object[] arr = new Object[stype.fieldNames().length];
		for (int i = 0; i < fields.length; i++)
			arr[i] = r.getAs(fields[i]);
		return RowFactory.create(arr);
	}
}
