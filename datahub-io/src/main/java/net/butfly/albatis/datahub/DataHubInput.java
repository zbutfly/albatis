package net.butfly.albatis.datahub;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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
	
	private final BlockingQueue<Consumer> consumers;
	
	private final Consumer consumer;
	private String table;
	
	{
		consumers = new LinkedBlockingQueue<>();
	}
	
	public DataHubInput(String name, URISpec uri, String... tables) throws IOException {
		super(name);
		this.table = tables[0];
		ConsumerConfig config = new ConsumerConfig("http://"+uri.getHost(), uri.getUsername(), uri.getPassword());
		consumer = new Consumer(uri.getFile(), tables[0], "1", config);
		consumers.add(consumer);
		closing(this::close);
	}

	@SuppressWarnings("deprecation")
	@Override
	public Rmap dequeue(){
		while (opened()) {
			Consumer c = consumers.poll();
			if(null != c) {
				RecordEntry entry = c.read(1);
				consumers.add(c);
				RecordData rdata = entry.getRecordData();
				if(rdata == null ) return null;
				if (rdata instanceof TupleRecordData) {
					Rmap rs =  new Rmap();
					TupleRecordData data = (TupleRecordData) rdata;
					data.getRecordSchema().getFields().forEach(f ->{
						rs.put(f.getName(), data.getField(f.getName()));
					});
					return new Rmap(table, null,"km", rs);
				}else {
					BlobRecordData data = (BlobRecordData) rdata;
					return new Rmap(table, null,"km", new String(data.getData(), Charsets.UTF_8));
				}
			}
		}
		return null;
	}
	
	@Override
	public void close() {
		consumer.close();
	}
}
