package net.butfly.albatis.hbase;

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
import scala.Tuple2;

public final class HbaseOutput extends FailoverOutput<HbaseResult, Result> {
	private static final long serialVersionUID = 2141020043117686747L;
	private final Connection connect;
	private final Map<String, Table> tables;

	public HbaseOutput(String name, String failoverPath) throws IOException {
		super(name, failoverPath, 100, 20);
		this.connect = Hbases.connect();
		tables = new ConcurrentHashMap<>();
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
	protected void closeInternal() {
		for (Table t : tables.values())
			try {
				t.close();
			} catch (IOException e) {
				logger().error("Hbase table [" + t.getName().toString() + "] close failure", e);
			}
		try {
			connect.close();
		} catch (IOException e) {
			logger().error("Hbase close failure", e);
		}
	}

	@Override
	protected void write(String key, List<Result> values) throws Exception {
		table(key).put(Collections.transN(values, r -> {
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
		}));
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
