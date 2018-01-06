package net.butfly.albatis.hbase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Configs;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Message;

public final class HbaseInput extends Namedly implements Input<Message> {
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
		if (null != startRow && null != stopRow) scaner = htable.getScanner(new Scan(startRow, stopRow));
		else if (null != startRow) scaner = htable.getScanner(new Scan(startRow));
		else scaner = htable.getScanner(new Scan());
		scanerLock = new ReentrantReadWriteLock();
		ended = new AtomicBoolean(false);
		open();
	}

	public HbaseInput(String name, HbaseConnection conn, String table, Filter... filters) throws IOException {
		super(name);
		hconn = conn;
		htname = table;
		htable = hconn.table(table);

		Scan scan = new Scan();
		if (null != filters && filters.length > 0) {
			Filter filter = filters.length == 1 ? filters[0] : new FilterList(filters);
			logger().debug(name() + " filtered: " + filter.toString());
			scan = scan.setFilter(filter);
		}
		scaner = htable.getScanner(scan);
		scanerLock = new ReentrantReadWriteLock();
		ended = new AtomicBoolean(false);
		trace(Result.class).sizing(Result::getTotalSizeOfCells)//
				.step(Long.parseLong(Configs.gets(HbaseProps.INPUT_STATS_STEP, "-1")));
		open();
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
	public void dequeue(Consumer<Sdream<Message>> using, int batchSize) {
		if (!ended.get() && scanerLock.writeLock().tryLock()) {
			Result[] rs = null;
			try {
				rs = scaner.next(batchSize);
			} catch (Exception ex) {
				logger().warn("Hbase failed and continue: " + ex.toString());
			} finally {
				scanerLock.writeLock().unlock();
			}
			if (null != rs) {
				ended.set(rs.length == 0);
				if (rs.length > 0) {
					using.accept(stats(Sdream.of(rs)).map(r -> Hbases.Results.result(htname, r)));
				}
			}
		}
	}
}
