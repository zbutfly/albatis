package net.butfly.albatis.redis;

import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;

public class RedisLRangeInput extends net.butfly.albacore.base.Namedly implements OddInput<Rmap> {

	public RedisLRangeInput(String name) {
		super(name);
	}
	private static final long serialVersionUID = -1411141076610748159L;

	@Override
	public Rmap dequeue() {
		// TODO Auto-generated method stub
		return null;
	}
}
