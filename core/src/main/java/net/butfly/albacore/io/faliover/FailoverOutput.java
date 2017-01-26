package net.butfly.albacore.io.faliover;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import net.butfly.albacore.io.OutputImpl;
import scala.Tuple2;

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
public abstract class FailoverOutput<I, FV> extends OutputImpl<I> {
	private final Failover<String, FV> failover;

	public FailoverOutput(String name, String failoverPath, int packageSize, int parallenism) throws IOException {
		super(name);
		if (failoverPath == null) failover = new HeapFailover<String, FV>(name(), kvs -> write(kvs._1, kvs._2), this::commit, packageSize,
				parallenism);
		else failover = new OffHeapFailover<String, FV>(name(), kvs -> write(kvs._1, kvs._2), this::commit, failoverPath, null, packageSize,
				parallenism) {
			private static final long serialVersionUID = -6942345655578531843L;

			@Override
			protected byte[] toBytes(String key, FV value) throws IOException {
				return FailoverOutput.this.toBytes(key, value);
			}

			@Override
			protected Tuple2<String, FV> fromBytes(byte[] bytes) throws IOException {
				return FailoverOutput.this.fromBytes(bytes);
			}
		};
	}

	protected abstract int write(String key, Collection<FV> values);

	protected void commit(String key) {}

	protected byte[] toBytes(String key, FV value) throws IOException {
		if (null == key || null == value) return null;
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(baos);) {
			oos.writeObject(unparse(key, value));
			return baos.toByteArray();
		} catch (IOException e) {
			logger().error("Serializing failure", e);
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	protected Tuple2<String, FV> fromBytes(byte[] bytes) throws IOException {
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
	public final boolean enqueue(I e, boolean block) {
		return enqueue(Arrays.asList(e)) == 1;
	}

	@Override
	public final long enqueue(List<I> els) {
		return failover.insertTask(els.stream().filter(e -> null != e).map(e -> parse(e)).collect(Collectors.groupingBy(t -> t._1,
				Collectors.mapping(t -> t._2, Collectors.toList()))));
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

	public final int tasks() {
		return failover.tasks();
	}
}
