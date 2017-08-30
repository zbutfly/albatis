package net.butfly.albatis.io.ext;

import net.butfly.albatis.io.OutputOddImpl;

public class ConsoleOutput extends OutputOddImpl<String> {
	@Override
	protected boolean enqueue(String item) {
		System.out.println(item);
		return true;
	}
}
