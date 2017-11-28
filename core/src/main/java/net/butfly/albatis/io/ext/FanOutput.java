package net.butfly.albatis.io.ext;

import static net.butfly.albacore.utils.collection.Streams.map;

import java.util.stream.Collectors;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.steam.Sdream;
import net.butfly.albatis.io.Output;

public class FanOutput<V> extends Namedly implements Output<V> {
	private final List<Consumer<Sdream<V>>> tasks;
	private final List<? extends Output<V>> outputs;

	public FanOutput(Iterable<? extends Output<V>> outputs) {
		this("FanOutTo" + ":" + map(outputs, o -> o.name(), Collectors.joining("&")), outputs);
		open();
	}

	public FanOutput(String name, Iterable<? extends Output<V>> outputs) {
		super(name);
		tasks = Colls.list();
		this.outputs = Colls.list(outputs);
		for (Output<V> o : outputs)
			tasks.add(items -> {
				o.enqueue(items);
			});
	}

	@Override
	public void enqueue(Sdream<V> s) {
		for (Output<V> o : outputs)
			o.enqueue(s);
	}
}
