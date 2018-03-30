package net.butfly.albatis.hbase;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Message;

public class HbaseInput extends HbaseBasicInput<Message> {
	public HbaseInput(String name, HbaseConnection conn) {
		super(name, conn);
	}

	protected Sdream<Message> m(Sdream<Message> m) {
		return m;
	};
}
