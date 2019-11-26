package net.butfly.albatis.parquet.impl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class PartitionStrategySingleField extends PartitionStrategy {
	private final String partitionField;
	private final String parsingFormat;
	private final SimpleDateFormat parsing;
	private final SimpleDateFormat partition;

	public PartitionStrategySingleField(String partitionField, String partitionFormat, String parsingFormat) {
		super();
		this.partitionField = partitionField;
		this.parsingFormat = parsingFormat;
		this.parsing = null == parsingFormat ? null : new SimpleDateFormat(parsingFormat);
		this.partition = new SimpleDateFormat(partitionFormat);
	}

	@Override
	public String partition(Map<String, Object> rec) {
		Object value = rec.get(partitionField);
		if (null == value) throw new IllegalArgumentException("Partition failed on value null.");
		Class<? extends Object> c = value.getClass();
		if (Date.class.isAssignableFrom(c)) return partition.format(value);
		if (Number.class.isAssignableFrom(c)) return partition.format(new Date(((Number) value).longValue()));
		if (CharSequence.class.isAssignableFrom(c)) if (null == parsing) //
			throw new IllegalArgumentException("Partition failed for string value without parseing format.");
		else try {
			return partition.format(parsing.parse(value.toString()));
		} catch (ParseException e) {
			throw new IllegalArgumentException("Partition failed with format [" + parsingFormat + "] failed on value: " + value.toString());
		}
		throw new IllegalArgumentException("Partition failed on value: " + value.toString());
	}
}
