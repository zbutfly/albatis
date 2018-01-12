package net.butfly.albatis.io.ext;

import java.util.UUID;

import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.OddInput;

public final class RandomStringInput extends net.butfly.albacore.base.Namedly implements OddInput<String> {
	public RandomStringInput() {
		super("RandomInput");
		open();
	}

	public static final Input<String> INSTANCE = new RandomStringInput();

	@Override
	public String dequeue() {
		return UUID.randomUUID().toString();
	}
}
