package net.butfly.albatis.io.ext;

import static net.butfly.albacore.utils.collection.Streams.map;

import java.util.stream.Collectors;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.steam.Sdream;
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
	public void enqueue(Sdream<V> s) {
		for (Output<V> o : outputs)
			o.enqueue(s);
	}
}
