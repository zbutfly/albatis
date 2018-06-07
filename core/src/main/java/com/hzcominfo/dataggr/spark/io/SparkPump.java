package com.hzcominfo.dataggr.spark.io;

import java.io.Serializable;

import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.Output;

public class SparkPump extends SparkPumpImpl<Message, SparkPump> implements Serializable {
	private static final long serialVersionUID = -6842560101323305087L;

	private final SparkInput input;
	private final Output<Message> output;

	public SparkPump(SparkInput input, int parallelism, Output<Message> output) {
		super(input.name() + ">" + output.name(), parallelism);
		this.input = input;
		this.output = output;
		Reflections.noneNull("Pump source/destination should not be null", input, output);
		depend(Colls.list(input, output));
		pumping(input::empty, this::p);
	}

	private void p() {
		if (opened())
			input.dequeue(output::enqueue);
	}
}
