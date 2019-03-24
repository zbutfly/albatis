package net.butfly.albatis.hbase;

import static net.butfly.albatis.hbase.HbaseInput.SCAN_BYTES;
import static net.butfly.albatis.hbase.HbaseInput.SCAN_CACHE_BLOCKS;
import static net.butfly.albatis.hbase.HbaseInput.SCAN_COLS;
import static net.butfly.albatis.hbase.HbaseInput.SCAN_SKIP;
import static net.butfly.albatis.hbase.Hbases.scan0;
import static net.butfly.albatis.hbase.Hbases.ScanOption.opt;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;

import net.butfly.albacore.utils.collection.Colls;

class HbaseTableScaner {
	final HbaseInput input;
	final String table;
	final String logicalName;
	final ResultScanner scaner;

	public HbaseTableScaner(HbaseInput input, String table, String logicalTable, Filter f, byte[]... startAndEndRow) {
		super();
		input.SCAN_REGS.computeIfAbsent(table, t -> Colls.list()).add(this);
		input.SCAN_POOL.add(this);
		this.input = input;
		this.table = table;
		this.logicalName = logicalTable;
		if (startAndEndRow.length == 0 && null != SCAN_SKIP)
			startAndEndRow = new byte[][] { input.hconn.skip(table, SCAN_SKIP.v1(), SCAN_SKIP.v2(), f) };
		Scan sc = opt(input.batchSize(), SCAN_COLS, SCAN_BYTES, SCAN_CACHE_BLOCKS).optimize(scan0(f, startAndEndRow));
		try {
			scaner = input.hconn.table(table).getScanner(sc);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void close() {
		try {
			scaner.close();
		} catch (Exception e) {} finally {
			input.SCAN_REGS.remove(table);
		}
	}

	public Result[] next(int batchSize) {
		try {
			return scaner.next(batchSize);
		} catch (IOException e) {
			return null;
		}
	}

	public Result next() {
		try {
			return scaner.next();
		} catch (IOException e) {
			return null;
		}
	}

	public boolean empty() {
		return !scaner.iterator().hasNext();
	}
}
