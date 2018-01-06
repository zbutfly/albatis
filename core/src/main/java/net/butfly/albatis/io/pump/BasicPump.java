package net.butfly.albatis.io.pump;

import net.butfly.albacore.utils.Reflections;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;

public class BasicPump<V> extends PumpImpl<V, BasicPump<V>> {
	private final Input<V> input;
	private final Output<V> output;

	public BasicPump(Input<V> input, int parallelism, Output<V> output) {
		super(input.name() + ">" + output.name(), parallelism);
		this.input = input;
		this.output = output;
		Reflections.noneNull("Pump source/destination should not be null", input, output);
		depend(input, output);
		pumping(input::empty, this::p);
	}

	private void p() {
		if (opened()) //
			input.dequeue(output::enqueue, batchSize);
	}
}
