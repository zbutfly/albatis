package net.butfly.albatis.impl.kafka.context;

public class ResultContext {
	public static final String RESULT_SUCCESS = "SUCCESS";

	public static final String RESULT_INIT_ALREADY_RUN = "ALREADY RUNNING";

	public static final String RESULT_INIT_NONE_TOPICS = "NONE TOPICS";

	public static final String RESULT_INIT_CONFIG_NULL = "NO CONNECT ADDRESS OR NO GROUP ID";

	public static final String RESULT_INIT_CONN_ERROR = "ZOOKEEPER CONNECT UNSUCCESS";
}
