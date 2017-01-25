package net.butfly.albatis.hbase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.IOs;
import scala.Tuple2;

public final class HbaseOutput extends FailoverOutput<HbaseResult, Result> {
	private final Connection connect;
	private final Map<String, Table> tables;

	public HbaseOutput(String name, String failoverPath) throws IOException {
		super(name, failoverPath, 100, 20);
		this.connect = Hbases.connect();
		tables = new ConcurrentHashMap<>();
		open();
	}

	private Table table(String table) {
		return tables.computeIfAbsent(table, t -> {
			try {
				return connect.getTable(TableName.valueOf(t));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
	}

	@Override
	protected void closeInternal() {
		for (String k : tables.keySet())
			try {
				Table t = tables.remove(k);
				if (null != t) t.close();
			} catch (IOException e) {
				logger().error("Hbase table [" + k + "] close failure", e);
			}
		try {
			synchronized (connect) {
				if (!connect.isClosed()) connect.close();
			}
		} catch (IOException e) {
			logger().error("Hbase close failure", e);
		}
	}

	@Override
	protected int write(String key, List<Result> values) {
		try {
			List<Put> t = Collections.transN(values, r -> {
				Put p;
				try {
					p = new Put(r.getRow());
				} catch (Exception ex) {
					logger().warn("Hbase converting failure, ignored and continued.", ex);
					return null;
				}
				for (Cell c : r.listCells())
					try {
						p.add(c);
					} catch (Exception e) {
						logger().warn("Hbase cell converting failure, ignored and continued.", e);
					}
				return p;
			});
			table(key).put(t);
			return t.size();
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected byte[] toBytes(String key, Result value) throws IOException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();) {
			IOs.writeBytes(baos, key.getBytes());
			IOs.writeBytes(baos, Hbases.resultToBytes(value));
			return baos.toByteArray();
		}
	}

	@Override
	protected Tuple2<String, Result> fromBytes(byte[] bytes) throws IOException {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);) {
			return new Tuple2<>(new String(IOs.readBytes(bais)), Hbases.resultFromBytes(IOs.readBytes(bais)));
		}
	}

	@Override
	protected Tuple2<String, Result> parse(HbaseResult e) {
		return new Tuple2<>(e.getTable(), e.getResult());
	}

	@Override
	protected HbaseResult unparse(String key, Result value) {
		return new HbaseResult(key, value);
	}
}
