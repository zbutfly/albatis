package net.butfly.albatis.io.ext;
//package net.butfly.albatis.io;
//
//import java.util.Map;
//import java.util.NoSuchElementException;
//import net.butfly.albacore.io.lambda.Function;
//
//import org.mapdb.Serializer;
//
//public class MdbMapQueue<V> extends MapdbQueueImpl<V> {
//	private final Function<V, String> keying;
//	private Map<String, byte[]> impl;
//
//	protected MdbMapQueue(String name, long capacity, String filename, Function<V, String> keying, Function<V, byte[]> conv,
//			Function<byte[], V> unconv) {
//		super(name, capacity, filename, conv, unconv);
//		this.keying = keying;
//		try {
//			impl = db.createTreeMap(name()).valueSerializer(Serializer.BYTE_ARRAY).makeStringMap();
//		} catch (IllegalArgumentException ex) {
//			impl = db.getTreeMap(name());
//		}
//		open();
//	}
//
//	@Override
//	public long size() {
//		return impl.size();
//	}
//
//	@Override
//	protected V dequeue() {
//		String k;
//		byte[] buf;
//		V v = null;
//		do {
//			try {
//				k = impl.keySet().iterator().next();
//			} catch (NoSuchElementException e) {
//				return null;
//			}
//		} while ((null == (buf = impl.remove(k)) || (null != (v = unconv.apply(buf)))) && opened() && !empty() && waitSleep());
//		return v;
//	}
//
//	@Override
//	protected boolean enqueue(V item) {
//		if (null == item) return false;
//		do {} while (opened() && full() && waitSleep());
//		byte[] v = conv.apply(item);
//		String k = keying.apply(item);
//		if (null == k || "".equals(k)) return false;
//		if (null == v) return false;
//		impl.put(k, v);
//		return true;
//	}
//}