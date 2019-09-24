package net.butfly.albatis.elastic;

import net.butfly.albatis.io.Rmap;
import org.apache.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;

import static net.butfly.albatis.io.Rmap.Op.*;

public class Elastics {
	public static DocWriteRequest<?> forWrite(Rmap m) {
		if (null == m.key()) return null;
		switch (m.op()) {
		case UPDATE:
			return new UpdateRequest(m.table().name, m.key().toString()).doc(m).docAsUpsert(false);
		case UPSERT:
			return new UpdateRequest(m.table().name, m.key().toString()).doc(m).docAsUpsert(true);
		case INSERT:
			return new IndexRequest(m.table().name).id(m.key().toString()).source(m);
		case DELETE:
			return new DeleteRequest(m.table().name, m.key().toString());
		default:
			return null;
		}
	}
}
