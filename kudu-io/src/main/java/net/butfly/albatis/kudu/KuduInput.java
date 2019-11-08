package net.butfly.albatis.kudu;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Rmap;

public class KuduInput extends Namedly implements Input<Rmap> {
	private static final long serialVersionUID = 2766579669550942687L;
	private final KuduConnectionBase<?, ?, ?> conn;
	private static KuduTable kuduTable;
	private final BlockingQueue<TableScanner> scanners = new LinkedBlockingQueue<>();
	private final Map<String, TableScanner> scannerMap = Maps.of();

	public KuduInput(String name, KuduConnectionBase<?, ?, ?> conn) {
		super(name);
		this.conn = conn;
		closing(this::closeKudu);
	}

	@Override
	public void open() {
		if (scannerMap.isEmpty() && null != conn.uri().getFile()) table(TableDesc.dummy(conn.uri().getFile()));
		Input.super.open();
	}

	private void closeKudu() {
		TableScanner s;
		while (!scannerMap.isEmpty()) if (null != (s = scanners.poll())) s.close();
	}

	void table(TableDesc table) {
		scannerMap.compute(table.qualifier.name, (t, existed) -> {
			if (null != existed) {
				logger().error("Table [" + table + "] input existed and conflicted, ignore new scan request.");
				return existed;
			}
			TableScanner s = new TableScanner(table);
			scanners.offer(s);
			return s;
		});
	}

	private class TableScanner {
		final TableDesc table;
		AsyncKuduScanner scanner;
		List<String> filterField = Colls.list();
		private Map<String, Type> schema = Maps.of();

		public TableScanner(TableDesc table) {
			super();
			this.table = table;
			kuduTable = conn.table(table.qualifier.name);
			List<ColumnSchema> columns = kuduTable.getSchema().getColumns();
			for (FieldDesc field : table.fields()) {
				filterField.add(field.name);
			}
			columns.stream().filter(t -> filterField.contains(t.getName())).forEach(c -> schema.put(c.getName(), c.getType()));
			scanner = kuduTable.getAsyncClient().newScannerBuilder(kuduTable).setProjectedColumnNames(Colls.list(schema.keySet())).build();
		}

		public void close() {
			try {
				scanner.close();
			} catch (Exception e) {
				logger().error("close kudu client exception", e);
			} finally {
				scannerMap.remove(table.qualifier.name);
			}
		}
	}

	@Override
	public Statistic statistic() {
		return new Statistic(this).sizing(KuduScanner::getLimit).<KuduScanner> infoing(ks -> {
			try {
				return ks.nextRows().next().rowToString();
			} catch (KuduException e) {
				logger().error("trace numRow and row data exception", e);
				return null;
			}
		});
	}

	@Override
	public boolean empty() {
		return scannerMap.isEmpty();
	}

	@Override
	public void dequeue(Consumer<Sdream<Rmap>> using) {
		TableScanner s;
		RowResultIterator rs;
		while (opened() && !empty()) if (null != (s = scanners.poll()) && null != (rs = scan(s))) {
			String keyField = s.table.keys.get(0).get(0);
			List<Rmap> rl = Colls.list();
			while (rs.hasNext()) {
				RowResult row = rs.next();
				Rmap r = new Rmap(s.table.qualifier);
				s.schema.forEach((f, t) -> {
					Object v = KuduCommon.getValue(row, f, t);
					if (null != v) r.put(f, v);
				});
				if (!Colls.empty(r)) {
					if (null != keyField) r.keyField(keyField);
					rl.add(r);
				}
			}
			if (!Colls.empty(rl)) {
				logger().trace("Kudu fetched [" + rl.size() + "] records, total [" + total.addAndGet(rl.size()) + "]");
				using.accept(Sdream.of(rl));
				return;
			}
		}
	}

	private RowResultIterator scan(TableScanner s) {
		RowResultIterator rs = null;
		try {
			if (!s.scanner.hasMoreRows()) {
				s.close();
				s = null;
			} else try {
				rs = s.scanner.nextRows().join();
				if (null == rs) s = null;
			} catch (Exception e) {
				logger().error("Kudu fail, reconnect.", e);
				// s.scanner = kuduTable.getAsyncClient().newScannerBuilder(kuduTable)//
				// .setProjectedColumnNames(Colls.list(s.schema.keySet())).build();
			}
		} finally {
			if (null != s) scanners.offer(s);
		}
		return rs;
	}

	private static final AtomicLong total = new AtomicLong();
}
