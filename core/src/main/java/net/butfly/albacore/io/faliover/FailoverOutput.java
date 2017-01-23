package net.butfly.albacore.io.faliover;

import net.butfly.albacore.io.Output;
import scala.Tuple2;

import java.io.*;
import java.util.*;

/**
 * Output with buffer and failover supporting.<br>
 * Parent class handle buffer, invoking really write/marshall op by callback
 * provided by children classes.<br>
 * Children classes define and implemented connection to datasource.
 * 
 * @author zx
 *
 * @param <I>
 * @param <FV>
 */
public abstract class FailoverOutput<I, FV> extends Output<I> {
	private static final long serialVersionUID = 6327633226368591655L;
	private final Failover<String, FV> failover;

	public FailoverOutput(String name, String failoverPath, int packageSize, int parallenism) throws IOException {
		super(name);
		if (failoverPath == null) failover = new HeapFailover<String, FV>(name(), (k, vs) -> write(k, vs), k -> commit(k), packageSize,
				parallenism);
		else failover = new OffHeapFailover<String, FV>(name(), (k, vs) -> write(k, vs), k -> commit(k), failoverPath, null, packageSize,
				parallenism) {
			private static final long serialVersionUID = -6942345655578531843L;

			@Override
			protected byte[] toBytes(String key, FV value) {
				return FailoverOutput.this.toBytes(key, value);
			}

			@Override
			protected Tuple2<String, FV> fromBytes(byte[] bytes) {
				return FailoverOutput.this.fromBytes(bytes);
			}
		};
	}

	protected abstract Exception write(String key, List<FV> values);

	protected void commit(String key) {}

	protected final byte[] toBytes(String key, FV value) {
		if (null == key || null == value) return null;
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(baos);) {
			oos.writeObject(unparse(key, value));
			return baos.toByteArray();
		} catch (IOException e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	protected final Tuple2<String, FV> fromBytes(byte[] bytes) {
		if (null == bytes) return null;
		try {
			return parse((I) new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject());
		} catch (ClassNotFoundException | IOException e) {
			return null;
		}
	}

	protected abstract Tuple2<String, FV> parse(I e);

	protected abstract I unparse(String key, FV value);

	@Override
	public final boolean enqueue0(I e) {
		return enqueue(Arrays.asList(e)) == 1;
	}

	@Override
	public final long enqueue(List<I> els) {
		Map<String, List<FV>> map = new HashMap<>();
		for (I e : els)
			if (null != e) {
				Tuple2<String, FV> t = parse(e);
				map.computeIfAbsent(t._1, core -> new ArrayList<>()).add(t._2);
			}
		failover.dotry(map);
		return els.size();
	}

	@Override
	public final void close() {
		super.close();
		failover.close();
		closeInternal();
	}

	protected abstract void closeInternal();

	public final long fails() {
		return failover.size();
	}
}
