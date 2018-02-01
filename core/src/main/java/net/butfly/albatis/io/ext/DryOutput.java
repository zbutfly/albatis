package net.butfly.albatis.io.ext;

import java.util.List;

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
		if (null != items) {
			List<V> l = items.list();
			if (!l.isEmpty()) {//
				s().stats(l);
				StringBuilder s = new StringBuilder("Mapping detail: ");
				for (V v : l)
					s.append("\n\t").append(v.toString());
				logger().trace(() -> s);
			}
		}
	}
}
