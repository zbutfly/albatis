package net.butfly.albatis.hbase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import net.butfly.albacore.io.Output;
import net.butfly.albacore.io.faliover.Failover;
import net.butfly.albacore.io.faliover.HeapFailover;
import net.butfly.albacore.io.faliover.OffHeapFailover;
import net.butfly.albacore.lambda.ConverterPair;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.logger.Logger;
import scala.Tuple2;

public final class HbaseOutput extends Output<HbaseResult> {
	private static final long serialVersionUID = 2141020043117686747L;
	private static final Logger logger = Logger.getLogger(HbaseOutput.class);
	private static final int DEFAULT_PARALLELISM = 5;
	private static final int DEFAULT_PACKAGE_SIZE = 500;
	private final Connection connect;

	private final Map<String, Table> tables;
	private final Failover<String, Result> failover;

	public HbaseOutput(String name, String failoverPath) throws IOException {
		super(name);
		this.connect = Hbases.connect();
		tables = new ConcurrentHashMap<>();
		ConverterPair<String, List<Result>, Exception> adding = (table, results) -> {
			try {
				table(table).put(Collections.transform(results, r -> new Put(r.getRow())));
				return null;
			} catch (IOException e) {
				return e;
			}
		};
		if (failoverPath == null) failover = new HeapFailover<String, Result>(name(), adding, DEFAULT_PACKAGE_SIZE, DEFAULT_PARALLELISM);
		else failover = new OffHeapFailover<String, Result>(name(), adding, failoverPath, null, DEFAULT_PACKAGE_SIZE, DEFAULT_PARALLELISM) {
			private static final long serialVersionUID = 7620077959670870367L;

			@Override
			protected byte[] toBytes(String core, Result doc) {
				if (null == core || null == doc) return null;
				try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(baos);) {
					oos.writeObject(new HbaseResult(core, doc));
					return baos.toByteArray();
				} catch (IOException e) {
					return null;
				}
			}

			@Override
			protected Tuple2<String, Result> fromBytes(byte[] bytes) {
				if (null == bytes) return null;
				try {
					HbaseResult sm = (HbaseResult) new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject();
					return new Tuple2<>(sm.getTable(), sm.getResult());
				} catch (ClassNotFoundException | IOException e) {
					return null;
				}
			}
		};
	}

	private Table table(String table) {
		return tables.computeIfAbsent(table, t -> {
			try {
				return connect.getTable(TableName.valueOf(t));
			} catch (IOException e) {
				return null;
			}
		});
	}

	@Override
	public void closing() {
		super.closing();
		failover.close();
		for (Table t : tables.values())
			try {
				t.close();
			} catch (IOException e) {
				logger.error("Hbase table [" + t.getName().toString() + "] close failure", e);
			}
		try {
			connect.close();
		} catch (IOException e) {
			logger.error("Hbase close failure", e);
		}
	}

	@Override
	public boolean enqueue0(HbaseResult result) {
		return enqueue(Arrays.asList(result)) == 1;
	}

	@Override
	public long enqueue(List<HbaseResult> results) {
		Map<String, List<Result>> map = new HashMap<>();
		for (HbaseResult r : results)
			if (null != r) map.computeIfAbsent(r.getTable(), core -> new ArrayList<>()).add(r.getResult());
		AtomicInteger count = new AtomicInteger(0);
		failover.doWithFailover(map, (t, rs) -> {
			List<Put> puts = new ArrayList<>();
			for (HbaseResult r : results) {
				if (null != r) try {
					puts.add(r.put());
				} catch (IOException e) {}
			}
			count.set(puts.size());
			if (puts.isEmpty()) return;
			try {
				table(t).put(puts);
			} catch (IOException e) {
				logger.error("Hbase output failure", e);
			}
		}, null);
		return count.get();
	}

	public long fails() {
		return 0;
	}
}
