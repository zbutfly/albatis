package net.butfly.albatis.io;

import static net.butfly.albacore.utils.collection.Streams.map;
import static net.butfly.albacore.utils.collection.Streams.of;
import static net.butfly.albacore.utils.collection.Streams.spatial;
import static net.butfly.albacore.utils.collection.Streams.spatialMap;
import static net.butfly.albacore.utils.parallel.Parals.eachs;

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import com.bluejeans.bigqueue.BigQueue;

import net.butfly.albacore.utils.collection.Its;

/**
 * Rich feature queue for big data processing, supporting:
 * <ul>
 * <li>Blocking based on capacity</li>
 * <li>Batching</li>
 * <ul>
 * <li>Batching in restrict synchronous or not</li>
 * </ul>
 * <li>Storage/pooling policies</li>
 * <ul>
 * <li>Instant</li>
 * <li>Memory (heap)</li>
 * <li>Local disk (off heap based on memory mapping), like {@link MapDB}/
 * {@link BigQueue} and so on</li>
 * <li>Remote, like Kafka/MQ and so on (Not now)</li>
 * </ul>
 * <li>Continuous or not</li>
 * <li>Connect to another ("then op", into engine named "Pump")</li>
 * <ul>
 * <li>Fan out to others ("thens op", to {@link KeyQueue})</li>
 * <li>Merge into {@link KeyQueue}</li>
 * </ul>
 * <li>Statistic</li>
 * </ul>
 * 
 * @author butfly
 */
public interface Queue0<I, O> extends Input<O>, Output<I> {
	static final long INFINITE_SIZE = -1;

	@Override
	long size();

	/* from interfaces */

	@Override
	default <O1> Queue0<I, O1> then(Function<O, O1> conv) {
		Queue0<I, O1> i = new Queue0<I, O1>() {
			@Override
			public void dequeue(Consumer<Stream<O1>> using, int batchSize) {
				Queue0.this.dequeue(s -> using.accept(map(s, conv)), batchSize);
			}

			@Override
			public void enqueue(Stream<I> items) {
				Queue0.this.enqueue(items);
			}

			@Override
			public void failed(Stream<I> failed) {
				Queue0.this.failed(failed);
			}

			@Override
			public void succeeded(long c) {
				Queue0.this.succeeded(c);
			}

			@Override
			public long size() {
				return Queue0.this.size();
			}
		};
		i.open();
		return i;
	}

	@Override
	default <O1> Queue0<I, O1> thens(Function<Iterable<O>, Iterable<O1>> conv, int parallelism) {
		Queue0<I, O1> i = new Queue0<I, O1>() {
			@Override
			public void dequeue(Consumer<Stream<O1>> using, int batchSize) {
				Queue0.this.dequeue(s -> spatialMap(s, parallelism, t -> conv.apply(() -> Its.it(t)).spliterator()).forEach(s1 -> using
						.accept(s1)), batchSize);
			}

			@Override
			public void enqueue(Stream<I> items) {
				Queue0.this.enqueue(items);
			}

			@Override
			public void failed(Stream<I> failed) {
				Queue0.this.failed(failed);
			}

			@Override
			public void succeeded(long c) {
				Queue0.this.succeeded(c);
			}

			@Override
			public long size() {
				return Queue0.this.size();
			}
		};
		i.open();
		return i;
	}

	@Override
	default <I0> Queue0<I0, O> prior(Function<I0, I> conv) {
		Queue0<I0, O> o = new Queue0<I0, O>() {
			@Override
			public void dequeue(Consumer<Stream<O>> using, int batchSize) {
				Queue0.this.dequeue(using, batchSize);
			}

			@Override
			public void enqueue(Stream<I0> items) {
				Queue0.this.enqueue(map(items, conv));
			}

			@Override
			public void failed(Stream<I0> failed) {
				Queue0.this.failed(map(failed, conv));
			}

			@Override
			public void succeeded(long c) {
				Queue0.this.succeeded(c);
			}

			@Override
			public long size() {
				return Queue0.this.size();
			}
		};
		o.open();
		return o;
	}

	@Override
	default <I0> Queue0<I0, O> priors(Function<Iterable<I0>, Iterable<I>> conv, int parallelism) {
		Queue0<I0, O> o = new Queue0<I0, O>() {
			@Override
			public void dequeue(Consumer<Stream<O>> using, int batchSize) {
				Queue0.this.dequeue(using, batchSize);
			}

			@Override
			public void enqueue(Stream<I0> items) {
				eachs(spatial(items, parallelism).values(), //
						(Consumer<Spliterator<I0>>) s0 -> Queue0.this.enqueue(of(conv.apply((Iterable<I0>) () -> Its.it(s0)))));
			}

			@Override
			public void succeeded(long c) {
				Queue0.this.succeeded(c);
			}

			@Override
			public void failed(Stream<I0> failed) {
				Queue0.this.failed(of(conv.apply((Iterable<I0>) () -> Its.it(failed.spliterator()))));
			}

			@Override
			public long size() {
				return Queue0.this.size();
			}
		};
		o.open();
		return o;
	}
}
