package net.butfly.albatis.impl.kafka.config;

import java.io.Serializable;

public class KafkaConsumerConfig implements Serializable {
	private static final long serialVersionUID = -3028341800709486625L;
	private String zookeeperConnect = "";
	private int zookeeperConnectionTimeoutMs = 15000;
	private int zookeeperSyncTimeMs = 5000;
	private String groupId = "";
	private boolean autoCommitEnable = false;
	private int autoCommitIntervalMs = 1000;
	private String autoOffsetReset = "smallest";
	private int sessionTimeoutMs = 30000;
	private String partitionAssignmentStrategy = "range";
	private int socketReceiveBufferBytes = 5 * 1024 * 1024;
	private int fetchMessageMaxBytes = 3 * 1024 * 1024;

	public String getZookeeperConnect() {
		return zookeeperConnect;
	}

	public void setZookeeperConnect(String zookeeperConnect) {
		this.zookeeperConnect = zookeeperConnect;
	}

	public int getZookeeperConnectionTimeoutMs() {
		return zookeeperConnectionTimeoutMs;
	}

	public void setZookeeperConnectionTimeoutMs(int zookeeperConnectionTimeoutMs) {
		this.zookeeperConnectionTimeoutMs = zookeeperConnectionTimeoutMs;
	}

	public int getZookeeperSyncTimeMs() {
		return zookeeperSyncTimeMs;
	}

	public void setZookeeperSyncTimeMs(int zookeeperSyncTimeMs) {
		this.zookeeperSyncTimeMs = zookeeperSyncTimeMs;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public boolean isAutoCommitEnable() {
		return autoCommitEnable;
	}

	public void setAutoCommitEnable(boolean autoCommitEnable) {
		this.autoCommitEnable = autoCommitEnable;
	}

	public int getAutoCommitIntervalMs() {
		return autoCommitIntervalMs;
	}

	public void setAutoCommitIntervalMs(int autoCommitIntervalMs) {
		this.autoCommitIntervalMs = autoCommitIntervalMs;
	}

	public String getAutoOffsetReset() {
		return autoOffsetReset;
	}

	public void setAutoOffsetReset(String autoOffsetReset) {
		this.autoOffsetReset = autoOffsetReset;
	}

	public int getSessionTimeoutMs() {
		return sessionTimeoutMs;
	}

	public void setSessionTimeoutMs(int sessionTimeoutMs) {
		this.sessionTimeoutMs = sessionTimeoutMs;
	}

	public String getPartitionAssignmentStrategy() {
		return partitionAssignmentStrategy;
	}

	public void setPartitionAssignmentStrategy(String partitionAssignmentStrategy) {
		this.partitionAssignmentStrategy = partitionAssignmentStrategy;
	}

	public int getSocketReceiveBufferBytes() {
		return socketReceiveBufferBytes;
	}

	public void setSocketReceiveBufferBytes(int socketReceiveBufferBytes) {
		this.socketReceiveBufferBytes = socketReceiveBufferBytes;
	}

	public int getFetchMessageMaxBytes() {
		return fetchMessageMaxBytes;
	}

	public void setFetchMessageMaxBytes(int fetchMessageMaxBytes) {
		this.fetchMessageMaxBytes = fetchMessageMaxBytes;
	}
}
