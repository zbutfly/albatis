package net.butfly.albacore.io.mapdb;
//package net.butfly.albacore.io;
//
//import java.util.concurrent.BlockingQueue;
//import java.util.function.Function;
//
//import org.mapdb.Serializer;
//
//import net.butfly.albacore.utils.parallel.Concurrents;
//
//public class MdbQueue<V> extends MapdbQueueImpl<V> {
//	private BlockingQueue<byte[]> impl;
//
//	protected MdbQueue(String name, long capacity, String filename, Function<V, byte[]> conv, Function<byte[], V> unconv) {
//		super(name, capacity, filename, conv, unconv);
//		try {
//			impl = db.createQueue(name(), Serializer.BYTE_ARRAY, true);
//		} catch (IllegalArgumentException ex) {
//			impl = db.getQueue(name());
//		}
//		open();
//	}
//
//	@Override
//	protected final boolean enqueue(V e) {
//		if (null == e) return false;
//		byte[] buf = conv.apply(e);
//		if (buf == null) return false;
//		do {} while (opened() && !impl.offer(buf) && Concurrents.waitSleep());
//		return false;
//	}
//
//	@Override
//	protected V dequeue() {
//		if (!opened()) return null;
//		byte[] buf = null;
//		do {} while (opened() && (buf = impl.poll()) == null && Concurrents.waitSleep());
//		return buf == null ? null : unconv.apply(buf);
//	}
//
//	@Override
//	public long size() {
//		return impl.size();
//	}
//}
