package net.butfly.albatis.io.ext;

import java.util.List;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Output;

public class DryOutput<V> extends Namedly implements Output<V> {
	private static final long serialVersionUID = -6732200800387125903L;

	public DryOutput(String inputName) {
		super(inputName + "DryOutput");
		logger().warn("Dry output from [" + inputName + "]");
	}

	@Override
	public void enqueue(Sdream<V> items) {
		if (null != items) {
			List<V> l = items.list();
			if (!l.isEmpty()) {//
				StringBuilder s = new StringBuilder("Mapping detail: ");
				for (V v : l)
					s.append("\n\t").append(v.toString());
				logger().trace(() -> s);
			}
		}
	}
}
