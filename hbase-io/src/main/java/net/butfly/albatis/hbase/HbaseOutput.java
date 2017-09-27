package net.butfly.albatis.hbase;

import static net.butfly.albacore.utils.collection.Colls.list;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Exceptions;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.Message.Op;

/**
 * Subject (embedded document serialized as BSON with prefixed column name) writer to hbase.
 */
public final class HbaseOutput extends OutputBase<Message> {
	public static final @HbaseProps String MAX_CONCURRENT_OP_PROP_NAME = HbaseProps.OUTPUT_CONCURRENT_OPS;
	public static final int MAX_CONCURRENT_OP_DEFAULT = Integer.MAX_VALUE;
	public static final int SUGGEST_BATCH_SIZE = 200;
	private final HbaseConnection hconn;

	public HbaseOutput(String name, HbaseConnection hconn, Function<Map<String, Object>, byte[]> ser) throws IOException {
		super(name);
		this.hconn = hconn;
	}

	@Override
	public long enqueue(String table, Stream<Message> values) throws EnqueueException {
		List<Pair<Message, ? extends Row>> l = incs(table, values).stream().filter(Streams.NOT_NULL).map(v -> new Pair<>(v, Hbases.Results
				.put(v))).filter(p -> null != p && null != p.v2()).collect(Collectors.toList());
		if (l.isEmpty()) return 0;
		List<Message> vs = l.stream().map(v -> v.v1()).collect(Collectors.toList());
		List<? extends Row> puts = l.stream().map(v -> v.v2()).collect(Collectors.toList());
		Object[] results = new Object[puts.size()];
		EnqueueException eex = new EnqueueException();
		try {
			s().statsOuts(enqs, c -> {
				try {
					hconn.table(table).batch(enqs, results);
				} catch (Exception e) {
					String err = Exceptions.unwrap(e).getMessage();
					err = err.replaceAll("\n\\s+at .*\\)\n", ""); // shink
																	// stacktrace
																	// in error
																	// message
					logger().debug(name() + " write failed [" + err + "], [" + enqs.size() + "] into fails.");
					failed(Sdream.of(origins));
				}
			});
		} finally {
			for (int i = 0; i < results.length; i++)
				if (results[i] instanceof Result) eex.success(1);
				else eex.fail(vs.get(i), results[i] instanceof Throwable ? (Throwable) results[i]
						: new RuntimeException("Unknown hbase return [" + results[i].getClass() + "]: " + results[i].toString()));
		}
	}

	private List<Message> incs(String table, Stream<Message> values) {
		Map<Boolean, List<Message>> ms = values.parallel().filter(Streams.NOT_NULL).collect(Collectors.partitioningBy(m -> m
				.op() == Op.INCREASE));
		List<Message> alls = ms.remove(Boolean.FALSE);
		Map<String, List<Message>> incByKeys = ms.get(Boolean.TRUE).parallelStream().collect(Collectors.groupingBy(m -> m.key()));
		for (Map.Entry<String, List<Message>> e : incByKeys.entrySet()) {
			Message merge = new Message(table, e.getKey());
			for (Message m : e.getValue())
				for (Map.Entry<String, Object> f : m.entrySet())
					merge.compute(f.getKey(), (fn, v) -> lvalue(v) + lvalue(e.getValue()));
			alls.add(merge);
		}
		return alls;
	}

	private long lvalue(Object o) {
		return null != o && Number.class.isAssignableFrom(o.getClass()) ? ((Number) o).longValue() : 0;
	}

	@Override
	public String partition(Message v) {
		return v.key();
	}
}
