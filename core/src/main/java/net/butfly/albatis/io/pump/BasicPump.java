package net.butfly.albatis.io.pump;

import net.butfly.albacore.utils.Reflections;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;

public class BasicPump<V> extends PumpImpl<V, BasicPump<V>> {
	public BasicPump(Input<V> input, int parallelism, Output<V> output) {
		super(input.name() + ">" + output.name(), parallelism);
		Reflections.noneNull("Pump source/destination should not be null", input, output);
		depend(input, output);
		logger().debug("Trace force on batch less than [" + forceTrace + "].");
		pumping(() -> input.empty(), () -> pumpOne(input, output));
	}

	private void pumpOne(Input<V> input, Output<V> output) {
		if (opened()) input.dequeue(s -> output.enqueue(stats(s)), batchSize);
	}
}
