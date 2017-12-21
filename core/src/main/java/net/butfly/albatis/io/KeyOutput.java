package net.butfly.albatis.io;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.ext.FailoverOutput;

public interface KeyOutput<K, V> extends Output<V> {
	K partition(V v);

	void enqueue(K key, Sdream<V> v);

	@Override
	public default void enqueue(Sdream<V> s) {
		s.partition((k, ss) -> enqueue(k, ss), v -> partition(v), 350);
	}

	@Override
	default FailoverKeyOutput<K, V> failover(Queue<V> pool, int batchSize) {
		return new FailoverKeyOutput<K, V>(this, pool, batchSize);
	}

	@Override
	default KeyOutput<K, V> batch(int batchSize) {
		KeyOutput<K, V> origin = this;
		KeyOutput<K, V> ko = new KeyOutput<K, V>() {
			@Override
			public K partition(V v) {
				return origin.partition(v);
			}

			@Override
			public void enqueue(K key, Sdream<V> s) {
				s.batch(ss -> origin.enqueue(key, ss), batchSize);
			}
		};
		ko.opening(() -> origin.open());
		ko.closing(() -> origin.close());
		return ko;
	}

	default void commit(K key) {
		commit();
	}

	@SuppressWarnings("unchecked")
	class FailoverKeyOutput<K, V> extends FailoverOutput<V> implements KeyOutput<K, V> {
		public FailoverKeyOutput(KeyOutput<K, V> output, Queue<V> failover, int batchSize) {
			super(output, failover, batchSize);
		}

		@Override
		public K partition(V v) {
			return ((KeyOutput<K, V>) base).partition(v);
		}

		@Override
		public void enqueue(K key, Sdream<V> v) {
			((KeyOutput<K, V>) base).enqueue(key, v);
		}
	}
}
