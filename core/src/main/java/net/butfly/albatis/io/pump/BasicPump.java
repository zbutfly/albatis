package net.butfly.albatis.io.pump;

import net.butfly.albacore.utils.Reflections;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;

public class BasicPump<V> extends PumpImpl<V, BasicPump<V>> {
	public BasicPump(Input<V> input, int parallelism, Output<V> output) {
		super(input.name() + ">" + output.name(), parallelism);
		Reflections.noneNull("Pump source/destination should not be null", input, output);
		depend(input, output);
		pumping(() -> input.empty(), () -> {
			if (opened()) input.dequeue(s -> output.enqueue(stats(s)), batchSize);
		});
	}
}
