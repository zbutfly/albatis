package net.butfly.albatis.datahub;

import java.io.IOException;

import org.apache.commons.codec.Charsets;

import com.aliyun.datahub.client.model.BlobRecordData;
import com.aliyun.datahub.client.model.RecordData;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.TupleRecordData;
import com.aliyun.datahub.clientlibrary.config.ConsumerConfig;
import com.aliyun.datahub.clientlibrary.consumer.Consumer;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;

public class DataHubInput extends net.butfly.albacore.base.Namedly implements OddInput<Rmap> {
	private static final long serialVersionUID = 2242853649760090074L;
	//private static final Logger logger = Logger.getLogger(DataHubInput.class);
	
	private final Consumer consumer;
	
	public DataHubInput(String name, URISpec uri, String... tables) throws IOException {
		super(name);
		ConsumerConfig config = new ConsumerConfig("http://"+uri.getHost(), uri.getUsername(), uri.getPassword());
		consumer = new Consumer(uri.getFile(), tables[0], "1", config);
		closing(this::close);
	}

	@Override
	public Rmap dequeue(){
		while (opened()) {
			Rmap rs =  new Rmap();
			RecordEntry entry = consumer.read(1);
			RecordData rdata = entry.getRecordData();
			if(rdata == null ) return null;
			if (rdata instanceof TupleRecordData) {
				TupleRecordData data = (TupleRecordData) rdata;
				data.getRecordSchema().getFields().forEach(f ->{
					rs.put(f.getName(), data.getField(f.getName()));
				});
			}else {
				BlobRecordData data = (BlobRecordData) rdata;
				rs.put("BlobRecordData", new String(data.getData(), Charsets.UTF_8));
			}
			return rs;	
		}
		return null;
	}
	
	@Override
	public void close() {
		consumer.close();
	}
}
