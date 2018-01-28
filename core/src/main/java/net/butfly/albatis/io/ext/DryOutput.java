package net.butfly.albatis.io.ext;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;

public class DryOutput<V> extends Namedly implements Output<V> {
	public DryOutput(Input<V> input) {
		super(input.name() + "DryOutput");
	}

	@Override
	public void enqueue(Sdream<V> items) {
		if (null != items) s().stats(items.list());
	}
}
