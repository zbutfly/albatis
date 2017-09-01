package net.butfly.albatis.io.ext;

import net.butfly.albatis.io.OddOutput;

public class ConsoleOutput extends OddOutput<String> {
	@Override
	protected boolean enqueue(String item) {
		System.out.println(item);
		return true;
	}
}
