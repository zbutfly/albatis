package net.butfly.albatis.kafka;

import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public abstract class KafkaOut extends OutputBase<Rmap> {
	private static final long serialVersionUID = 8131758107073637552L;

	protected KafkaOut(String name) {
		super(name);
	}
}
