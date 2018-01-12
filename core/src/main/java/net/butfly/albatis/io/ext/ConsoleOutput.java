package net.butfly.albatis.io.ext;

import net.butfly.albatis.io.OddOutputBase;

public class ConsoleOutput extends OddOutputBase<String> {
	public ConsoleOutput() {
		super("ConsoleOutput");
	}

public class ConsoleOutput extends net.butfly.albacore.base.Namedly implements OddOutput<String> {
	public ConsoleOutput() {
		super("ConsoleOutput");
		open();
	}

	@Override
	public boolean enqueue(String item) {
		System.out.println(item);
		return true;
	}
}
