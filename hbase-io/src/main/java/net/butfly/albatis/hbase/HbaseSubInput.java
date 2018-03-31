package net.butfly.albatis.hbase;

import static net.butfly.albacore.paral.Sdream.of;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.hbase.HbaseSubInput.SubMessage;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.Wrapper;

/**
 * Subject (embedded document serialized as BSON with prefixed column name) reader from hbase.
 */
public class HbaseSubInput extends Wrapper.WrapInput<SubMessage, Message> {
	private final char colkeySplit;
	private final Function<byte[], Map<String, Object>> der;

	HbaseSubInput(Input<? extends Message> hbaseInput, Function<byte[], Map<String, Object>> conv, char colkeySplit) {
		super(hbaseInput, "Subject");
		this.der = conv;
		this.colkeySplit = colkeySplit;
	}

	@Override
	public void dequeue(Consumer<Sdream<SubMessage>> using) {
		base.dequeue(ms -> using.accept(ms.mapFlat(this::splitByPrefix)));
	}

	public class SubMessage extends Message {
		private static final long serialVersionUID = 4552898109478724531L;

		public final String cf;
		public final String colkey;
		public final String prefix;

		public SubMessage(String table, Object rowkey, String prefix, String colkey, String cf, Map<String, Object> data) {
			super(table, rowkey, data);
			put(".SUBJECT.COLKEY", colkey);
			put(".SUBJECT.COLPREFIX", prefix);
			put(".SUBJECT.COLFAMILY", cf);
			this.prefix = prefix;
			this.cf = cf;
			this.colkey = colkey;
		}

		@Override
		public synchronized SubMessage map(Map<String, Object> map) {
			return (SubMessage) super.map(map);
		}

		@Override
		public String toString() {
			return "[" + cf + ":" + prefix + colkeySplit + colkey + "]" + super.toString();
		}
	}

	public Sdream<SubMessage> splitByPrefix(Message m) {
		return of(m).map(c -> {
			String[] cfs = c.getKey().split(":", 2); // cf and prefix#colkey
			int sp = cfs[1].lastIndexOf(colkeySplit);
			String colkey = cfs[1].substring(sp + 1);
			String prefix = cfs[1].substring(0, sp);
			Map<String, Object> r = der.apply((byte[]) c.getValue());
			r.remove("_id");
			return new SubMessage(m.table(), m.key(), prefix, colkey, cfs[0], r);
		});
	}
}
