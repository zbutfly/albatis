package net.butfly.albatis.kafka;

public class KafkaException extends Exception {
	private static final long serialVersionUID = -480995519946047535L;

	public KafkaException() {
		super();
	}

	public KafkaException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public KafkaException(String message, Throwable cause) {
		super(message, cause);
	}

	public KafkaException(String message) {
		super(message);
	}

	public KafkaException(Throwable cause) {
		super(cause);
	}
}
