package net.butfly.albatis.parquet.impl;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.fs.Path;

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
