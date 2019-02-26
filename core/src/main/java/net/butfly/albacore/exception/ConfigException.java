package net.butfly.albacore.exception;

import java.io.IOException;

public class ConfigException extends IOException {
	private static final long serialVersionUID = -480995519946047535L;

	public ConfigException() {
		super();
	}

	public ConfigException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConfigException(String message) {
		super(message);
	}

	public ConfigException(Throwable cause) {
		super(cause);
	}
}
