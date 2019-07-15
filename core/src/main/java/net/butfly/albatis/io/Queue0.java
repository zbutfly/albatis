package net.butfly.albatis.io;

import com.bluejeans.bigqueue.BigQueue;

import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Sdream;

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
 * <li>Local disk (off heap based on memory mapping), like {@link MapDB}/ {@link BigQueue} and so on</li>
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
			private static final long serialVersionUID = 2094498345260970342L;

			@Override
			public void dequeue(Consumer<Sdream<O1>> using) {
				Queue0.this.dequeue(s -> using.accept(s.map(conv)));
			}

			@Override
			public void enqueue(Sdream<I> items) {
				Queue0.this.enqueue(items);
			}

			@Override
			public void failed(Sdream<I> failed) {
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
	default <O1> Queue0<I, O1> thens(Function<Sdream<O>, Sdream<O1>> conv) {
		// TODO Auto-generated method stub
		return null;
	}

	@Deprecated
	@Override
	default <O1> Queue0<I, O1> thens(Function<Sdream<O>, Sdream<O1>> conv, int parallelism) {
		Queue0<I, O1> i = new Queue0<I, O1>() {
			private static final long serialVersionUID = -1318189091743592868L;

			@Override
			public void dequeue(Consumer<Sdream<O1>> using) {
				Queue0.this.dequeue(s -> s.partition(s1 -> using.accept(conv.apply(s1)), parallelism));
			}

			@Override
			public void enqueue(Sdream<I> items) {
				Queue0.this.enqueue(items);
			}

			@Override
			public void failed(Sdream<I> failed) {
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
	default <O1> Queue0<I, O1> thenFlat(Function<O, Sdream<O1>> conv) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	default <I0> Queue0<I0, O> prior(Function<I0, I> conv) {
		Queue0<I0, O> o = new Queue0<I0, O>() {
			private static final long serialVersionUID = -1672582499335537429L;

			@Override
			public void dequeue(Consumer<Sdream<O>> using) {
				Queue0.this.dequeue(using);
			}

			@Override
			public void enqueue(Sdream<I0> s) {
				Queue0.this.enqueue(s.map(conv));
			}

			@Override
			public void failed(Sdream<I0> failed) {
				Queue0.this.failed(failed.map(conv));
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
	default <I0> Queue0<I0, O> priors(Function<Sdream<I0>, Sdream<I>> conv) {
		// TODO Auto-generated method stub
		return null;
	}

	@Deprecated
	@Override
	default <I0> Queue0<I0, O> priors(Function<Sdream<I0>, Sdream<I>> conv, int parallelism) {
		Queue0<I0, O> o = new Queue0<I0, O>() {
			private static final long serialVersionUID = 1513309707964507542L;

			@Override
			public void dequeue(Consumer<Sdream<O>> using) {
				Queue0.this.dequeue(using);
			}

			@Override
			public void enqueue(Sdream<I0> s) {
				s.partition(ss -> Queue0.this.enqueue(conv.apply(ss)), parallelism);
			}

			@Override
			public void succeeded(long c) {
				Queue0.this.succeeded(c);
			}

			@Override
			public void failed(Sdream<I0> failed) {
				failed.partition(ss -> Queue0.this.failed(conv.apply(failed)), parallelism);
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
	default <I0> Queue0<I0, O> priorFlat(Function<I0, Sdream<I>> conv) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	default Queue0<I, O> stats() {
		return this;
	}
}
