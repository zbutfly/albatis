package net.butfly.albatis.solr;

import org.apache.solr.common.SolrDocumentBase;

import net.butfly.albacore.io.Message;
import net.butfly.albacore.utils.Systems;

public class SolrMessage<D extends SolrDocumentBase<?, D>> extends Message<String, D, SolrMessage<D>> {
	private static final long serialVersionUID = -3391502515682546301L;
	private String core;
	private D doc;

	public SolrMessage(String core, D doc) {
		super();
		this.core = core;
		this.doc = doc;
	}

	public String getCore() {
		return core;
	}

	@Override
	public D forWrite() {
		return doc;
	}

	public long size() {
		return Systems.sizeOf(doc);
	}

	@Override
	public String partition() {
		return core;
	}

	@Override
	public String id() {
		return doc.toString();
	}

	@Override
	public String toString() {
		return doc.toString();
	}
}
