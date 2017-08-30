package net.butfly.albatis.io.ext;

import java.util.UUID;

import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.InputOddImpl;

public final class RandomStringInput extends InputOddImpl<String> {
	public RandomStringInput() {
		super();
		open();
	}

	public static final Input<String> INSTANCE = new RandomStringInput();

	@Override
	protected String dequeue() {
		return UUID.randomUUID().toString();
	}
}
