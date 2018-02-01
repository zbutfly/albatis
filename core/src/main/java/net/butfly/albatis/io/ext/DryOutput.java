package net.butfly.albatis.io.ext;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Output;

public class DryOutput<V> extends Namedly implements Output<V> {
	public DryOutput(String inputName) {
		super(inputName + "DryOutput");
		logger().warn("Dry output from [" + inputName + "]");
	}

	@Override
	public void enqueue(Sdream<V> items) {
		if (null != items) s().stats(items.list());
	}
}
