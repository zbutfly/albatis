package net.butfly.albatis.io.vfs;

import java.util.Map;
import java.util.NoSuchElementException;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public class VfsOutput extends OutputBase<Rmap> {
	private static final long serialVersionUID = 7401929118128636464L;
	private final VfsConnection conn;
	private Map<String, VfsWriter> writers = Maps.of();

	public VfsOutput(VfsConnection conn) {
		super("VfsOutput");
		this.conn = conn;
	}

	@Override
	protected void enqsafe(Sdream<Rmap> items) {
		items.partition((t, s) -> s.eachs(writers.computeIfAbsent(t, tt -> new VfsWriter(conn, tt))::write), t -> t.table().name, -1);
	}

	@Override
	public void close() {
		VfsWriter w;
		while (!writers.isEmpty()) {
			String t;
			try {
				t = writers.keySet().iterator().next();
			} catch (NoSuchElementException e) {
				return;
			}
			if (null != (w = writers.remove(t))) w.close();
		}
	}
}
