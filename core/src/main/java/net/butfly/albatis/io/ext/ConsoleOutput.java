package net.butfly.albatis.io.ext;

import net.butfly.albatis.io.OddOutputBase;

public class ConsoleOutput extends OddOutputBase<String> {
	private static final long serialVersionUID = 7401929118128636464L;

	public ConsoleOutput() {
		super("ConsoleOutput");
	}

	@Override
	protected boolean enqueue0(String item) {
		System.out.println(item);
		return true;
	}
}
