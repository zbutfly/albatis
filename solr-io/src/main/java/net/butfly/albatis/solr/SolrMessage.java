package net.butfly.albatis.solr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import net.butfly.albacore.io.Message;
import net.butfly.albacore.utils.IOs;

public class SolrMessage extends Message {
	private static final long serialVersionUID = -3391502515682546301L;
	public final boolean delete;

	public SolrMessage(String core) {
		super(core);
		delete = false;
	}

	public SolrMessage(String core, Map<? extends String, ? extends Object> doc) {
		this(core, doc, false);
	}

	public SolrMessage(String core, Map<? extends String, ? extends Object> doc, boolean delete) {
		super(core, doc);
		this.delete = delete;
	}

	@Override
	protected void write(OutputStream os) throws IOException {
		super.write(os);
		IOs.writeBytes(os, new byte[] { (byte) (delete ? 1 : 0) });
	}

	public static SolrMessage fromBytes(byte[] b) {
		if (null == b) throw new IllegalArgumentException();
		try (ByteArrayInputStream bais = new ByteArrayInputStream(b)) {
			Message base = Message.fromBytes(bais);
			boolean delete = IOs.readBytes(bais)[0] == 1;
			return new SolrMessage(base.table(), base, delete);
		} catch (IOException e) {
			return null;
		}
	}
}
