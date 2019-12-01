package net.butfly.albatis.parquet.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.Rmap;

public class BigQueueSer {
	static byte[] ser(Rmap r) {
		try (ByteArrayOutputStream out = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(out);) {
			oos.writeObject(r);
			return out.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	static List<byte[]> ser(Collection<? extends Rmap> rr) {
		List<Future<byte[]>> ff = Colls.list();
		for (Rmap r : rr) ff.add(Exeter.of().submit(() -> ser(r)));
		List<byte[]> bb = Colls.list();
		for (Future<byte[]> f : ff) try {
			bb.add(f.get());
		} catch (InterruptedException e) {} catch (ExecutionException e) {
			BigBlockingQueue.logger.error("Big queue deserialization fail.", e);
		}
		return bb;
	}

	static Rmap der(byte[] b) {
		try (ByteArrayInputStream in = new ByteArrayInputStream(b); ObjectInputStream ois = new ObjectInputStream(in);) {
			return (Rmap) ois.readObject();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	static List<Rmap> der1(Collection<byte[]> rr) {
		List<Future<Rmap>> ff = Colls.list();
		for (byte[] r : rr) ff.add(Exeter.of().submit(() -> der(r)));
		List<Rmap> bb = Colls.list();
		for (Future<Rmap> f : ff) try {
			bb.add(f.get());
		} catch (InterruptedException e) {} catch (ExecutionException e) {
			BigBlockingQueue.logger.error("Big queue deserialization fail.", e);
		}
		return bb;
	}

	static List<Rmap> der(Collection<byte[]> rr) {
		List<Rmap> ff = Colls.list();
		for (byte[] r : rr) ff.add(der(r));
		return ff;
	}
}
