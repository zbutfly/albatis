package net.butfly.albatis.parquet.utils;

import static net.butfly.albatis.parquet.utils.BigQueueSer.der;
import static net.butfly.albatis.parquet.utils.BigQueueSer.ser;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.apache.log4j.Logger;

import com.bluejeans.bigqueue.BigQueue;

import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.Rmap;

public class BigBlockingQueue implements BlockingQueue<Rmap>, AutoCloseable {
	static final Logger logger = Logger.getLogger(BigBlockingQueue.class);
	public static final String BASE = Configs.gets("net.butfly.albatis.parquet.writer.cache.base.dir"); // "." + File.separator + "localdata"
	private static final boolean PURGING = Boolean.parseBoolean(Configs.gets("net.butfly.albatis.parquet.writer.cache.purge", "true"));
	private final String base;
	public final String dbfile;
	private final BigQueue pool;
	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	public BigBlockingQueue() {
		this(UUID.randomUUID().toString());
	}

	public BigBlockingQueue(String id) {
		super();
		this.base = null == BASE ? ("." + File.separator + "localdata") : BASE;
		dbfile = id;
		pool = new BigQueue(base, dbfile);
	}

	@Override
	public Rmap poll() {
		byte[] b = lockWrite(pool::dequeue);
		return null == b ? null : der(b);
	}

	@Override
	public Rmap remove() {
		Rmap r = poll();
		if (null == r) throw new NoSuchElementException();
		return r;
	}

	@Override
	public Rmap peek() {
		byte[] b = lockRead(pool::peek);
		return null == b ? null : der(b);
	}

	@Override
	public Rmap element() {
		Rmap r = peek();
		if (null == r) throw new NoSuchElementException();
		return r;
	}

	@Override
	public int size() {
		return lockRead(pool::size).intValue();
	}

	@Override
	public boolean isEmpty() { return lockRead(pool::isEmpty).booleanValue(); }

	@Override
	public Iterator<Rmap> iterator() {
		return new Iterator<Rmap>() {
			@Override
			public Rmap next() {
				return lockWrite(() -> BigBlockingQueue.this.remove());
			}

			@Override
			public boolean hasNext() {
				return lockRead(() -> size()) > 0;
			}
		};
	}

	@Override
	public Object[] toArray() {
		return lockWrite(() -> der(pool.dequeueMulti(Integer.MAX_VALUE)).toArray());
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return (T[]) lockWrite(() -> der(pool.dequeueMulti(Integer.MAX_VALUE)).toArray(a));
	}

	@Override
	public boolean addAll(Collection<? extends Rmap> c) {
		if (null == c || c.isEmpty()) return false;
		List<byte[]> bb = ser(c);
		lockWrite(() -> {
			for (byte[] b : bb) pool.enqueue(b);
		});
		return true;
	}

	@Override
	public void clear() {
		lockWrite(pool::removeAll);
	}

	@Override
	public boolean add(Rmap e) {
		return offer(e);
	}

	@Override
	public boolean offer(Rmap e) {
		byte[] b = ser(e);
		lockWrite(() -> pool.enqueue(b));
		return true;
	}

	@Override
	public boolean offer(Rmap e, long timeout, TimeUnit unit) throws InterruptedException {
		return offer(e);
	}

	@Override
	public void put(Rmap e) throws InterruptedException {
		offer(e);
	}

	@Override
	public Rmap take() throws InterruptedException {
		Rmap r;
		while (null == (r = poll()));
		return r;
	}

	@Override
	public Rmap poll(long timeout, TimeUnit unit) throws InterruptedException {
		Rmap r;
		long mills = unit.toMillis(timeout);
		long now = System.currentTimeMillis();
		while (null == (r = poll())) if (System.currentTimeMillis() - now > mills) return null;
		return r;
	}

	@Override
	public int remainingCapacity() {
		return Integer.MAX_VALUE;
	}

	@Override
	public boolean contains(Object o) {
		throw new UnsupportedOperationException();
	}

	private static final int DRAIN_BATCH_SIZE = 4096;

	@Override
	public int drainTo(Collection<? super Rmap> c) {
		List<Future<Rmap>> cc = Colls.list();
		lockWrite(() -> {
			List<byte[]> bb;
			while (!pool.isEmpty()) {
				bb = pool.dequeueMulti(DRAIN_BATCH_SIZE);
				if (bb.isEmpty()) break;
				bb.forEach(b -> cc.add(Exeter.of().submit(() -> der(b))));
			}
		});
		cc.forEach(f -> {
			try {
				c.add(f.get());
			} catch (InterruptedException e) {} catch (ExecutionException e) {
				logger.error("Big queue deserialization fail.", e);
			}
		});
		return cc.size();
	}

	@Override
	public int drainTo(Collection<? super Rmap> c, int maxElements) {
		List<byte[]> bb = lockWrite(() -> pool.dequeueMulti(maxElements));
		if (bb.isEmpty()) return 0;
		List<Rmap> cc = der(bb);
		c.addAll(cc);
		return cc.size();
	}

	public int drainAtLeast(Collection<? super Rmap> c, int minElements) {
		List<byte[]> bb = lockWrite(() -> {
			if (pool.size() < minElements) return null;
			else {
				long now = System.currentTimeMillis();
				try {
					return pool.dequeueMulti(minElements);
				} finally {
					logger.trace("Big queue [" + dbfile + "] dequeue " + minElements + " recs in " + (System.currentTimeMillis() - now)
							+ " ms.");
				}
			}
		});
		if (null == bb || bb.isEmpty()) return 0;
		List<Rmap> cc;
		long now = System.currentTimeMillis();
		try {
			cc = der(bb);
		} finally {
			logger.trace("Big queue [" + dbfile + "] deserialized " + bb.size() + " recs in " + (System.currentTimeMillis() - now) + " ms.");
		}
		c.addAll(cc);
		return cc.size();
	}

	@Override
	public void close() throws IOException {
		try {
			try {
				pool.gc();
			} finally {
				pool.close();
			}
		} finally {
			if (PURGING) purge();
		}
	}

	public void purge() throws IOException {
		Files.walk(Paths.get(base, dbfile)).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
	}

	public long diskSize() {
		return lockRead(() -> folderSize(Paths.get(base, dbfile).toFile()));
	}

	private static long folderSize(File dir) {
		long length = 0;
		for (File file : dir.listFiles()) {
			if (file.isFile()) length += file.length();
			else length += folderSize(file);
		}
		return length;
	}

	private <T> T lockWrite(Supplier<T> doing) {
		Lock l = lock.writeLock();
		l.lock();
		try {
			return doing.get();
		} finally {
			l.unlock();
		}
	}

	private void lockWrite(Runnable doing) {
		Lock l = lock.writeLock();
		l.lock();
		try {
			doing.run();
		} finally {
			l.unlock();
		}
	}

	private <T> T lockRead(Supplier<T> getting) {
		Lock l = lock.readLock();
		l.lock();
		try {
			return getting.get();
		} finally {
			l.unlock();
		}
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException();
	}
}
