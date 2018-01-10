package net.butfly.albatis.io.ext;

import static net.butfly.albacore.paral.Sdream.of;

import java.util.List;
import java.util.function.Consumer;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.Output;

public class FanOutput<V> extends Namedly implements Output<V> {
	private final List<Consumer<Sdream<V>>> tasks;
	private final Iterable<? extends Output<V>> outputs;

	public FanOutput(Iterable<? extends Output<V>> outputs) {
		this("FanOutTo" + ":" + of(outputs).joinAsString(Output::name, "&"), outputs);
		open();
	}

	@Override
	public void close() {
		Output.super.close();
		for (Output<V> o : outputs)
			o.close();
	}

	public FanOutput(String name, Iterable<? extends Output<V>> outputs) {
		super(name);
		tasks = Colls.list();
		this.outputs = outputs;
		for (Output<V> o : outputs)
			tasks.add(items -> {
				o.enqueue(items);
			});
	}

	@Override
	public void enqueue(Sdream<V> s) {
		Exeter.of().join(s, tasks);
	}
}
