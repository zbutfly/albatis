package net.butfly.albatis.hbase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Message;

public final class HbaseInput extends Namedly implements Input<Message> {
	private final long SCAN_BYTES = Props.propL(HbaseInput.class, "scan.bytes", 3145728); // 3M
	private final int SCAN_ROWS = Props.propI(HbaseInput.class, "scan.rows", 1);
	private final HbaseConnection hconn;
	private final String htname;
	private final Table htable;

	private ResultScanner scaner = null;
	private ReentrantReadWriteLock scanerLock;
	private AtomicBoolean ended;

	public HbaseInput(String name, HbaseConnection conn, String table) throws IOException {
		this(name, conn, table, null, null);
	}

	public HbaseInput(String name, HbaseConnection conn, String table, byte[] startRow, byte[] stopRow) throws IOException {
		super(name);
		hconn = conn;
		htname = table;
		htable = hconn.table(table);
		Scan sc;
		if (null != startRow && null != stopRow) sc = new Scan(startRow, stopRow);
		else if (null != startRow) sc = new Scan(startRow);
		else sc = new Scan();
		scaner = htable.getScanner(Hbases.optimize(sc, batchSize(), SCAN_ROWS, SCAN_BYTES));
		scanerLock = new ReentrantReadWriteLock();
		ended = new AtomicBoolean(false);
		open();
	}

	public HbaseInput(String name, HbaseConnection conn, String table, Filter... filters) throws IOException {
		super(name);
		hconn = conn;
		htname = table;
		htable = hconn.table(table);

		Scan sc = new Scan();
		if (null != filters && filters.length > 0) {
			Filter filter = filters.length == 1 ? filters[0] : new FilterList(filters);
			logger().debug(name() + " filtered: " + filter.toString());
			sc = sc.setFilter(filter);
		}
		scaner = htable.getScanner(Hbases.optimize(sc, batchSize(), SCAN_ROWS, SCAN_BYTES));
		scanerLock = new ReentrantReadWriteLock();
		ended = new AtomicBoolean(false);
		open();
	}

	@Override
	public Statistic trace() {
		return new Statistic(this).sizing(Result::getTotalSizeOfCells).<Result> sampling(r -> Bytes.toString(r.getRow()));
	}

	@Override
	public void close() {
		Input.super.close();
		try {
			if (null != scaner) scaner.close();
		} catch (Exception e) {}
		try {
			htable.close();
		} catch (Exception e) {}
		try {
			hconn.close();
		} catch (Exception e) {}
	}

	@Override
	public boolean empty() {
		return ended == null || ended.get();
	}

	@Override
	public void dequeue(Consumer<Sdream<Message>> using) {
		if (!ended.get() && scanerLock.writeLock().tryLock()) {
			Result[] rs = null;
			try {
				rs = s().statsInA(() -> {
					// logger().error("Hbase step [" + batchSize() + "] begin.");
					// long now = System.currentTimeMillis();
					try {
						return scaner.next(batchSize());
					} catch (Exception e) {
						logger().warn("Hbase failed and continue: " + e.toString());
						return null;
						// } finally {
						// logger().error("Hbase step [" + batchSize() + "] spent: " + (System.currentTimeMillis() - now) + " ms.");
					}
				});
			} finally {
				scanerLock.writeLock().unlock();
			}
			if (null != rs) {
				ended.set(rs.length == 0);
				if (rs.length > 0) {
					List<Message> ms = Colls.list();
					for (Result r : rs)
						if (null != r) ms.add(Hbases.Results.result(htname, r));
					using.accept(Sdream.of(ms));
				}
			}
		}
	}
}
