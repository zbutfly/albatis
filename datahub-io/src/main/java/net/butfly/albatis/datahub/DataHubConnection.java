package net.butfly.albatis.datahub;

import java.io.IOException;

import com.aliyun.datahub.client.DatahubClient;

import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.DataConnection;

public class DataHubConnection extends DataConnection<DatahubClient> {
//	private static final Logger logger = Logger.getLogger(DataHubConnection.class);
////datahub://YourAccessKeyId:YourAccessKeySecret@dh-cn-hangzhou.aliyuncs.com/YourProjectName
	protected DataHubConnection(URISpec uri, String... supportedSchema) throws IOException {
		super(uri, supportedSchema);
	}

	public DataHubConnection(URISpec uri) throws IOException {
		this(uri, "datahub");
	}
	
	
	@Override
	public DataHubInput input(String... tables) throws IOException {
		try {
			return new DataHubInput("DataHubInput", uri, tables);
		} catch (ConfigException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public DataHubOutput output(String... tables) throws IOException {
		return new DataHubOutput("DataHubOutput", uri, tables);
	}

}

/*	public DataHubConnection(URISpec uri) {
		DatahubConfig datahubConfig = new DatahubConfig("http://"+uri.getHost(), new AliyunAccount(uri.getUsername(), uri.getPassword());
		ClientManagerFactory.getClientManager(projectName, topicName, datahubConfig);
		DatahubClient datahubClient = DatahubClientBuilder.newBuilder()
				.setDatahubConfig(
						new DatahubConfig("http://"+uri.getHost(), new AliyunAccount(uri.getUsername(), uri.getPassword()), true))
				.setHttpConfig(new HttpConfig().setConnTimeout(10000)).build();
		dc = datahubClient;
	}*/