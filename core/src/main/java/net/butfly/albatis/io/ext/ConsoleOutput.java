package net.butfly.albatis.io.ext;

import net.butfly.albatis.io.OddOutputBase;

public class ConsoleOutput extends OddOutputBase<String> {
	public ConsoleOutput() {
		super("ConsoleOutput");
		open();
	}

	@Override
	protected boolean enqueue0(String item) {
		System.out.println(item);
		return true;
	}
}
