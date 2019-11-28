package net.butfly.albatis.parquet.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.bluejeans.bigqueue.BigQueue;

import net.butfly.albacore.utils.Configs;
import net.butfly.albatis.io.Rmap;

public class BigBlockingQueue implements BlockingQueue<Rmap>, AutoCloseable {
	private static final String BASE = Configs.gets("net.butfly.albatis.parquet.writer.cache.base.dir", "." + File.separator + "localdata");
	private static final boolean PURGING = Boolean.parseBoolean(Configs.gets("net.butfly.albatis.parquet.writer.cache.purge", "true"));
	public final String dbfile;
	private final BigQueue pool;

	public BigBlockingQueue(String id) {
		super();
		dbfile = id;
		pool = new BigQueue(BASE, dbfile);
	}

	public void enqueue(Rmap r) {
		pool.enqueue(ser(r));
	}

	public long diskSize() {
		return folderSize(Paths.get(BASE, dbfile).toFile());
	}

	private static long folderSize(File dir) {
		long length = 0;
		for (File file : dir.listFiles()) {
			if (file.isFile()) length += file.length();
			else length += folderSize(file);
		}
		return length;
	}

	@Override
	public Rmap remove() {
		Rmap r = poll();
		if (null == r) throw new NoSuchElementException();
		return r;
	}

	@Override
	public Rmap poll() {
		byte[] b = pool.dequeue();
		return null == b ? null : der(b);
	}

	@Override
	public Rmap element() {
		Rmap r = peek();
		if (null == r) throw new NoSuchElementException();
		return r;
	}

	@Override
	public Rmap peek() {
		byte[] b = pool.peek();
		return null == b ? null : der(b);
	}

	@Override
	public int size() {
		return (int) pool.size();
	}

	@Override
	public boolean isEmpty() { return pool.isEmpty(); }

	@Override
	public Iterator<Rmap> iterator() {
		return new Iterator<Rmap>() {

			@Override
			public Rmap next() {
				return BigBlockingQueue.this.remove();
			}

			@Override
			public boolean hasNext() {
				return size() > 0;
			}
		};
	}

	@Override
	public Object[] toArray() {
		return pool.dequeueMulti(Integer.MAX_VALUE).toArray();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T[] toArray(T[] a) {
		return (T[]) pool.dequeueMulti(Integer.MAX_VALUE).toArray(new Rmap[0]);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(Collection<? extends Rmap> c) {
		if (null == c || c.isEmpty()) return false;
		for (Rmap r : c) enqueue(r);
		return true;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		pool.removeAll();
		return true;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		pool.removeAll();
	}

	@Override
	public boolean add(Rmap e) {
		return offer(e);
	}

	@Override
	public boolean offer(Rmap e) {
		enqueue(e);
		return true;
	}

	@Override
	public void put(Rmap e) throws InterruptedException {
		offer(e);
	}

	@Override
	public boolean offer(Rmap e, long timeout, TimeUnit unit) throws InterruptedException {
		return offer(e);
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
	public boolean remove(Object o) {
		pool.dequeue();
		return true;
	}

	@Override
	public boolean contains(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int drainTo(Collection<? super Rmap> c) {
		byte[] b;
		while (null != (b = pool.dequeue())) c.add(der(b));
		return c.size();
	}

	@Override
	public int drainTo(Collection<? super Rmap> c, int maxElements) {
		for (byte[] b : pool.dequeueMulti(maxElements)) c.add(der(b));
		return c.size();
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
		Files.walk(Paths.get(BASE, dbfile)).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
	}

	private byte[] ser(Rmap r) {
		try (ByteArrayOutputStream out = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(out);) {
			oos.writeObject(r);
			return out.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private Rmap der(byte[] b) {
		try (ByteArrayInputStream in = new ByteArrayInputStream(b); ObjectInputStream ois = new ObjectInputStream(in);) {
			return (Rmap) ois.readObject();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
}
