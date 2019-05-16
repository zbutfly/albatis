package net.butfly.albatis.datahub;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.clientlibrary.config.ProducerConfig;
import com.aliyun.datahub.clientlibrary.producer.Producer;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

public class DataHubOutput extends OutputBase<Rmap> {
	private static final long serialVersionUID = -2376114954650957250L;
	private final Producer producer;
	private URISpec uri;
	private static final int maxRetry = 10;

	protected DataHubOutput(String name, URISpec uri, String... tables) {
		super(name);
		this.uri = uri;
		ProducerConfig config = new ProducerConfig("http://"+uri.getHost(), uri.getUsername(), uri.getPassword());
		producer = new Producer(uri.getFile(), tables[0],  config);
		closing(producer::close);
	}

	@Override
	public void close() {
		producer.close();
	}

	@Override
	protected void enqsafe(Sdream<Rmap> messages) {
		List<Rmap> records = messages.list();
		producer.send(records.stream().map(r -> conv(r)).collect(Collectors.toList()), maxRetry);
	}
	
	public RecordEntry conv(Rmap r) {
		RecordEntry rs = new RecordEntry();
		r.entrySet().forEach(kv ->{
			rs.addAttribute(kv.getKey(), kv.getKey());
		});
		return rs;
	}

	@Override
	public URISpec target() {
		return uri;
	}
}
