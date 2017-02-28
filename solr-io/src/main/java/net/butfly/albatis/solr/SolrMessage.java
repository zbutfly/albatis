package net.butfly.albatis.solr;

import org.apache.solr.common.SolrDocumentBase;

import net.butfly.albacore.io.Message;
import net.butfly.albacore.utils.Systems;

public class SolrMessage<D extends SolrDocumentBase<?, D>> extends Message<String, D, SolrMessage<D>> {
	private static final long serialVersionUID = -3391502515682546301L;
	public final boolean delete;
	private final String core;
	private final D doc;

	public SolrMessage(String core, D doc) {
		this(core, doc, false);
	}

	public SolrMessage(String core, D doc, boolean delete) {
		super();
		this.delete = delete;
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
	public String toString() {
		return core + ":\n\t" + doc.toString();
	}
}
