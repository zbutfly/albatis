package net.butfly.albatis.parquet.impl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.ExtraTableDesc.ExtraEnabled;

@ExtraEnabled
public class PartitionStrategyMultipleField extends PartitionStrategy {
	private final List<Segment> segments = Colls.list();

	@SuppressWarnings("unchecked")
	public PartitionStrategyMultipleField(Map<String, Object> json) {
		super();
		Map<String, Object> config = (Map<String, Object>) json.get("partitionConfig");
		rollingMS = config.containsKey("maxTimeHour") ? (long) (((Number) config.get("maxTimeHour")).doubleValue() * 3600 * 1000) : 0;
		rollingRecord = config.containsKey("maxRecordSize") ? ((Number) config.get("maxRecordSize")).intValue() : 0;
		rollingByte = config.containsKey("maxFileSize") ? ((Number) config.get("maxFileSize")).longValue() * 1024 * 1024 : 0;
		refreshMS = config.containsKey("refreshIntervalHour") ? //
				(long) (((Number) config.get("refreshIntervalHour")).doubleValue() * 3600 * 1000) : 0;
		hdfsUri = (String) config.get("hdfsUrl");
		// "incMaxTimeIntervalHour"
		if (json.containsKey("partitionColumnList")) //
			for (Map<String, Object> col : (List<Map<String, Object>>) json.get("partitionColumnList")) //
				segments.add(new Segment(col));
	}

	@Override
	public String partition(Map<String, Object> rec) {
		if (null == rec || rec.isEmpty()) throw new IllegalArgumentException("Partition failed on value null.");
		List<String> ss = new ArrayList<>();
		for (Segment seg : segments) {
			Object v = rec.get(seg.srcFieldName);
			if (null == v) throw new IllegalArgumentException("Partition failed on value null for field: " + seg.partitionFieldName);
			switch (seg.srcFieldType) {
			case DATE:
				ss.add(seg.partitionFieldName + "=" + seg(v, seg.partitionFormat, seg.parseFormat));
				break;
			default:
				throw new IllegalArgumentException();
			}
		}
		return String.join(Path.SEPARATOR, ss);
	}

	private String seg(Object value, String partitionFormat, String parseFormat) {
		Class<? extends Object> c = value.getClass();
		SimpleDateFormat f = new SimpleDateFormat(partitionFormat);
		if (Date.class.isAssignableFrom(c)) return f.format(value);
		if (Number.class.isAssignableFrom(c)) return f.format(new Date(((Number) value).longValue()));
		if (CharSequence.class.isAssignableFrom(c)) if (null == parseFormat) //
			throw new IllegalArgumentException("Partition failed for string value without parseing format.");
		else try {
			return f.format(new SimpleDateFormat(parseFormat).parse(value.toString()));
		} catch (ParseException e) {
			throw new IllegalArgumentException("Partition failed with format [" + parseFormat + "] failed on value: " + value.toString());
		}
		return null;
	}

	public static class Segment {
		public final int level;
		public final String partitionFieldName;
		public final FieldType partitionFieldType;
		public final String partitionFormat;

		public final String srcFieldName;
		public final FieldType srcFieldType;
		public final String parseFormat;

		public Segment(Map<String, Object> json) {
			super();
			level = ((Number) json.get("level")).intValue();
			parseFormat = (String) json.get("parseFormat");;
			partitionFieldName = (String) json.get("partitionFieldName");
			partitionFieldType = FieldType.valueOf(((String) json.get("partitionFieldType")).toUpperCase());
			partitionFormat = (String) json.get("partitionFormat");
			srcFieldName = (String) json.get("srcFieldName");
			srcFieldType = FieldType.valueOf(((String) json.get("srcFieldType")).toUpperCase());
		}
	}

	private enum FieldType {
		STRING, DATE
	}
}
