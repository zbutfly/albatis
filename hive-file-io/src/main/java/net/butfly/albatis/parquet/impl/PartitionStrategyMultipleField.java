package net.butfly.albatis.parquet.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import net.butfly.albacore.expr.Engine;
import net.butfly.albacore.utils.Configs;
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

	private static final int MAX_SEGS = Integer.parseInt(Configs.gets("net.butfly.albatis.parquet.output.seg.max", "-1"));

	@Override
	public String partition(Map<String, Object> rec) {
		if (null == rec || rec.isEmpty()) throw new IllegalArgumentException("Partition failed on value null or empty.");
		List<String> ss = new ArrayList<>();
		for (Segment seg : segments) ss.add(seg.seg(rec));
		if (MAX_SEGS > 0 && MAX_SEGS < ss.size()) ss = ss.subList(0, MAX_SEGS);
		return String.join(Path.SEPARATOR, ss);
	}

	public static class Segment {
		public final int level;
		public final String partitionFieldName;
		public final String srcField;

		public Segment(Map<String, Object> json) {
			super();
			level = ((Number) json.get("level")).intValue();
			partitionFieldName = (String) json.get("partitionFieldName");
			String expr = (String) json.get("srcField");
			if (expr.startsWith("=")) expr = expr.substring(1);
			srcField = expr;
		}

		public String seg(Map<String, Object> rec) {
			return partitionFieldName + "=" + Engine.eval(srcField, rec);
			// Class<? extends Object> c = value.getClass();
			// if (Date.class.isAssignableFrom(c)) return f.format(value);
			// if (Number.class.isAssignableFrom(c)) return f.format(new Date(((Number) value).longValue()));
			// if (CharSequence.class.isAssignableFrom(c)) // if (null == parseFormat) //
			// throw new IllegalArgumentException("Partition failed for string value without parseing format.");
			// else try {
			// return f.format(new SimpleDateFormat(parseFormat).parse(value.toString()));
			// } catch (ParseException e) {
			// throw new IllegalArgumentException("Partition failed without format on value: " + value.toString());
			// }
		}
	}

}
