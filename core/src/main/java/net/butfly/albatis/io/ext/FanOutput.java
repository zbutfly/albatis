package net.butfly.albatis.io.ext;

import static net.butfly.albacore.utils.collection.Streams.list;
import static net.butfly.albacore.utils.collection.Streams.map;
import static net.butfly.albacore.utils.collection.Streams.of;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.parallel.Task;
import net.butfly.albatis.io.Output;

public class FanOutput<V> extends Namedly implements Output<V> {
	private final Iterable<? extends Output<V>> outputs;

	public FanOutput(Iterable<? extends Output<V>> outputs) {
		this("FanOutTo" + ":" + map(outputs, o -> o.name(), Collectors.joining("&")), outputs);
		open();
	}

	public FanOutput(String name, Iterable<? extends Output<V>> outputs) {
		super(name);
		this.outputs = outputs;
	}

	@Override
	public void enqueue(Stream<V> items) {
		List<V> values = list(items);
		Task t = null;
		for (Output<V> o : outputs) {
			Task t0 = () -> o.enqueue(of(values));
			if (null == t) t = t0;
			else t = t.multiple(t0);
		}
		t.run();
	}
}
