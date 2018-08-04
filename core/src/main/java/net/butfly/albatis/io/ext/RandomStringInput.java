package net.butfly.albatis.io.ext;

import java.util.UUID;

import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.OddInput;

public final class RandomStringInput extends net.butfly.albacore.base.Namedly implements OddInput<String> {
	private static final long serialVersionUID = 3328419256355113001L;

	public RandomStringInput() {
		super("RandomInput");
	}

	public static final Input<String> INSTANCE = new RandomStringInput();

	@Override
	public String dequeue() {
		return UUID.randomUUID().toString();
	}
}
