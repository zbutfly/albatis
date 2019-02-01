package net.butfly.albatis.io.ext;

import static net.butfly.albacore.paral.Sdream.of;

import java.util.List;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.Output;

public class FanOutput<V> extends Namedly implements Output<V> {
	private static final long serialVersionUID = -162999699679518749L;
	private final List<Consumer<Sdream<V>>> tasks;
	private final List<? extends Output<V>> outputs;
	private final boolean concurrent;

	@Deprecated
	public FanOutput(Iterable<? extends Output<V>> outputs) {
		this(outputs, true);
	}

	public FanOutput(Iterable<? extends Output<V>> outputs, boolean concurrent) {
		this("FanOutTo" + ":" + of(outputs).joinAsString(Output::name, "&"), outputs, concurrent);
	}

	@Override
	public void open() {
		for (int i = outputs.size() - 1; i >= 0; i--)
			outputs.get(i).open();
		Output.super.open();
	}

	@Override
	public void close() {
		Output.super.close();
		for (Output<V> o : outputs)
			o.close();
	}

	public FanOutput(String name, Iterable<? extends Output<V>> outputs, boolean concurrent) {
		super(name);
		this.concurrent = concurrent;
		tasks = Colls.list();
		this.outputs = Colls.list(outputs);
		for (Output<V> o : outputs)
			tasks.add(l -> o.enqueue(l));
	}

	@Override
	public void enqueue(Sdream<V> s) {
		if (Colls.empty(tasks)) return;
		if (tasks.size() == 1) tasks.get(0).accept(s);
		List<V> l = s.list(); // sdream is one-time, so had to terminate it and construct new sdream
		if (concurrent) Exeter.of().join(of(l), tasks);
		else for (Consumer<Sdream<V>> t : tasks)
			t.accept(of(l));
	}
}
