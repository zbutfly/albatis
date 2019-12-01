package net.butfly.albatis.parquet.impl;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.fs.Path;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.parquet.HiveConnection;
import net.butfly.albatis.parquet.utils.BigBlockingQueue;

public class HiveParquetWriterHDFSWithCacheMM extends HiveParquetWriterHDFSWithCache {
	private final BigBlockingQueue pool;

	public HiveParquetWriterHDFSWithCacheMM(TableDesc table, HiveConnection conn, Path base) {
		super(table, conn, base);
		String fn = base.toString().replaceAll(Path.SEPARATOR, "-");
		if (fn.startsWith("-")) fn = fn.substring(1);
		pool = new BigBlockingQueue(fn);
	}

	@Override
	protected BlockingQueue<Rmap> pool() {
		return pool;
	}

	@Override
	public HiveParquetWriterHDFSWithCache rolling(boolean forcing) {
		List<Rmap> l = Colls.list();
		long now = System.currentTimeMillis();
		try {
			if (forcing) pool().drainTo(l);
			else pool.drainAtLeast(l, strategy.rollingRecord);
		} finally {
			if (!l.isEmpty()) {
				now = System.currentTimeMillis() - now;
				logger.trace("Parquet data " + l.size() + " drained and deserialized from cache " + "in " + now + " ms");
			}
		}
		if (l.isEmpty()) return this;

		Path p = new Path(partitionBase, filename());
		Thread t = new Thread(() -> upload(p, l), "ParquetWriting@" + p);
		t.setDaemon(false);
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
		return this;
	}

	@Override
	public void close() {
		super.close();
		try {
			pool.close();
		} catch (IOException e) {
			logger.error("Disk cache clean fail: " + pool.dbfile, e);
		}
	}

	@Override
	public long currentBytes() {
		return pool.diskSize();
	}
}
