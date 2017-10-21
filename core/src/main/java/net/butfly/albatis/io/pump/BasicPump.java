package net.butfly.albatis.io.pump;

import com.hzcominfo.albatis.Albatis;

import net.butfly.albacore.utils.Reflections;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;

public class BasicPump<V> extends PumpImpl<V, BasicPump<V>> {
	private static final int DEFAULT_BATCH_SIZE = Integer.parseInt(System.getProperty(Albatis.Props.PROP_PUMP_BATCH_SIZE, "1000"));

	public BasicPump(Input<V> input, int parallelism, Output<V> output) {
		this(input, parallelism, DEFAULT_BATCH_SIZE, output);
	}

	public BasicPump(Input<V> input, int parallelism, int batchSize, Output<V> output) {
		super(input.name() + ">" + output.name(), parallelism);
		this.input = input;
		this.output = output;
		Reflections.noneNull("Pump source/destination should not be null", input, output);
		depend(input, output);
		pumping(() -> input.empty(), () -> {
			if (opened()) input.dequeue(s -> output.enqueue(stats(s)), batchSize);
		});
	}
}
