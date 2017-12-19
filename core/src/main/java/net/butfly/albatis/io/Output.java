package net.butfly.albatis.io;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.hzcominfo.albatis.Albatis;

import net.butfly.albacore.io.Enqueuer;
import net.butfly.albacore.io.IO;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.ext.FailoverOutput;

public interface Output<V> extends IO, Consumer<Sdream<V>>, Enqueuer<V> {
	static Output<?> NULL = items -> {};

	@Override
	default long size() {
		return 0;
	}

	@Override
	default void accept(Sdream<V> items) {
		enqueue(items);
	}

	default <V0> Output<V0> prior(Function<V0, V> conv) {
		return Wrapper.wrap(this, "Prior", s -> enqueue(s.map(conv)));
	}

	default <V0> Output<V0> priors(Function<Sdream<V0>, Sdream<V>> conv) {
		return priors(conv, 1);
	}

	default <V0> Output<V0> priors(Function<Sdream<V0>, Sdream<V>> conv, int parallelism) {
		return Wrapper.wrap(this, "Priors", (Enqueuer<V0>) s -> s.partition(ss -> enqueue(conv.apply(ss)), parallelism));
	}

	default void commit() {}

	// more extends
	default Output<V> safe() {
		return new SafeOutput<>(this, detectMaxConcurrentOps(this, logger()));
	}

	default FailoverOutput<V> failover(Queue<V> pool, int batchSize) {
		return new FailoverOutput<V>(this, pool, batchSize);
	}

	default Output<V> batch(int batchSize) {
		return Wrapper.wrap(this, "Batch", s -> s.batch(this::enqueue, batchSize));
	}

	default <K> KeyOutput<K, V> partitial(Function<V, K> partitier, BiConsumer<K, Sdream<V>> enqueuing) {
		return new KeyOutput<K, V>() {
			@Override
			public K partition(V v) {
				return partitier.apply(v);
			}

			@Override
			public void enqueue(K key, Sdream<V> v) {
				enqueuing.accept(key, v);
			}
		};
	}

	// constructor
	static <T> Output<T> of(Collection<? super T> underly) {
		return s -> s.each(underly::add);
	}

	static int detectMaxConcurrentOps(Output<?> output, Logger logger) {
		Class<?> oc = (output instanceof Wrapper ? (Output<?>) ((Wrapper<?>) output).bases() : output).getClass();
		int cop = -1;
		Field f;
		int mod;
		try {
			if (null != (f = oc.getField(Albatis.MAX_CONCURRENT_OP_FIELD_NAME))) {
				mod = f.getModifiers();
				if (Modifier.isStatic(mod) && Modifier.isPublic(mod) && Modifier.isFinal(mod) //
						&& CharSequence.class.isAssignableFrom(f.getType())) {
					CharSequence conf;
					if (null != (conf = (CharSequence) f.get(null))) {
						int defv = -1;
						if (null != (f = oc.getField(Albatis.MAX_CONCURRENT_OP_FIELD_NAME_DEFAULT))) {
							mod = f.getModifiers();
							if (Modifier.isStatic(mod) && Modifier.isPublic(mod) && Modifier.isFinal(mod)) defv = f.getInt(null);
						}
						String confn = conf.toString();
						String info = "Output [" + oc.toString() + "] concurrent limit configurated by -D" + confn;
						cop = Integer.parseInt(Configs.of().gets(confn, Integer.toString(defv)));
						if (cop > 0) info += "=" + cop;
						if (defv > 0) info += " [default:" + defv + "]";
						logger.info(info + ".");
					}
				}
			}
		} catch (Exception e) {}
		return cop;
	}
}
