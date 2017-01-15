package net.butfly.albatis.solr;

import java.io.Serializable;

import org.apache.solr.common.SolrDocumentBase;

import net.butfly.albacore.utils.Systems;

public class SolrMessage<D extends SolrDocumentBase<?, D>> implements Serializable {
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

	public D getDoc() {
		return doc;
	}

	public long size() {
		return Systems.sizeOf(doc);
	}
}
