package net.butfly.albatis.io.ext;

import net.butfly.albatis.io.OddOutput;

public class ConsoleOutput extends net.butfly.albacore.base.Namedly implements OddOutput<String> {
	@Override
	public boolean enqueue(String item) {
		System.out.println(item);
		return true;
	}
}
