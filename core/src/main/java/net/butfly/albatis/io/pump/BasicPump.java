package net.butfly.albatis.io.pump;

import static net.butfly.albacore.utils.Reflections.noneNull;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;

public class BasicPump<V> extends PumpImpl<V, BasicPump<V>> {
	protected final Input<V> input;
	protected final Output<V> output;

	protected BasicPump(String name, Input<V> input, int parallelism, Output<V> output) {
		super(name, parallelism);
		noneNull("Pump source/destination should not be null", input, output);
		this.input = input;
		this.output = output;
		try {
			Class.forName("net.butfly.albacore.expr.Engine");
			Class.forName(net.butfly.albacore.expr.Engine.Default.class.getName());
		} catch (ClassNotFoundException e) {
			logger().error("init fel error!", e);
		}
		depend(Colls.list(input, output));
		pumping(input::empty, this::proc);
	}

	public BasicPump(Input<V> input, int parallelism, Output<V> output) {
		this(input.name() + ">" + output.name(), input, parallelism, output);
	}

	protected void proc() {
		if (opened()) input.dequeue(output);
	}
}
