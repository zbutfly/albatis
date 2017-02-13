package net.butfly.albatis.hbase;

import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.io.Streams;
import net.butfly.albacore.io.faliover.Failover.FailoverException;
import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.utils.IOs;
import scala.Tuple2;

public final class HbaseOutput extends FailoverOutput<HbaseResult, Result> {
	private final Connection connect;
	private final Map<String, Table> tables;

	public HbaseOutput(String name, String failoverPath) throws IOException {
		super(name, failoverPath, 500, 10);
		connect = Hbases.connect();
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
	protected int write(String key, Collection<Result> values) throws FailoverException {
		List<Put> puts = io.list(values, r -> {
			Put p;
			try {
				p = new Put(r.getRow());
			} catch (Exception ex) {
				logger().warn("Hbase converting failure, ignored and continued, row: " + Bytes.toString(r.getRow()), ex);
				return null;
			}
			for (Cell c : r.listCells())
				try {
					p.add(c);
				} catch (Exception e) {
					logger().warn("Hbase cell converting failure, ignored and continued, row: " + Bytes.toString(r.getRow()) + ", cell: "
							+ CellUtil.cloneFamily(c) + CellUtil.cloneQualifier(c), e);
				}
			return p;
		});
		try {
			table(key).put(puts);
		} catch (Exception e) {
			throw new FailoverException(io.collect(Streams.of(values), Collectors.toConcurrentMap(r -> r, r -> {
				String m = unwrap(e).getMessage();
				if (null == m) m = "Unknown message";
				return m;
			})));
		}
		return puts.size();
	}

	@Override
	protected byte[] toBytes(String key, Result value) throws IOException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();) {
			IOs.writeBytes(baos, key.getBytes());
			IOs.writeBytes(baos, Hbases.toBytes(value));
			return baos.toByteArray();
		}
	}

	@Override
	protected Tuple2<String, Result> fromBytes(byte[] bytes) throws IOException {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);) {
			return new Tuple2<>(new String(IOs.readBytes(bais)), Hbases.toResult(IOs.readBytes(bais)));
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
