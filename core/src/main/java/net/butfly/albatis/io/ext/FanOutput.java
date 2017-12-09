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

	public FanOutput(Iterable<? extends Output<V>> outputs) {
		this("FanOutTo" + ":" + of(outputs).joinAsString(Output::name, "&"), outputs);
		open();
	}

	public FanOutput(String name, Iterable<? extends Output<V>> outputs) {
		super(name);
		tasks = Colls.list();
		for (Output<V> o : outputs)
			tasks.add(s -> o.enqueue(s));
	}

	@Override
	public void enqueue(Sdream<V> s) {
		Exeter.of().join(s, tasks);
	}
}
