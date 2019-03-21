package net.butfly.albatis.io.ext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albatis.FileConnection;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public class FileOutput extends OutputBase<Rmap> {
	private static final long serialVersionUID = 7401929118128636464L;
	private final FileConnection fc;

	public FileOutput(FileConnection file) {
		super("FileOutput");
		this.fc = file;
	}

	@Override
	protected void enqsafe(Sdream<Rmap> items) {
		items.partition((t, s) -> {
			File f = new File(fc.root);
			if (!f.exists() && !f.mkdirs()) throw new RuntimeException();
			f = new File(f, t);
			try {
				if (!f.exists() && !f.createNewFile()) throw new RuntimeException();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			try (FileOutputStream o = new FileOutputStream(f, true); PrintWriter w = new PrintWriter(o);) {
				items.eachs(r -> w.println(JsonSerder.JSON_MAPPER.ser(r)));
			} catch (IOException e) {}
		}, r -> {
			String t = r.table().name;
			if (!t.endsWith(fc.ext)) t += fc.ext;
			return t;
		}, -1);
	}
}
