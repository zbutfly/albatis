package net.butfly.albatis.io.ext;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.event.Level;

import net.butfly.albacore.utils.Instances;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.OddOutputBase;

public final class LoggerOutput extends OddOutputBase<String> {
	private final Map<Level, Consumer<String>> loggings;
	private final Logger logger;
	private final Level level;

	public LoggerOutput() {
		this(Level.INFO);
	}

	public LoggerOutput(Class<?> clazz) {
		this(clazz.getName(), Level.INFO);
	}

	public LoggerOutput(Level level) {
		this(Thread.currentThread().getStackTrace()[2].getClassName(), level);
	}

	public LoggerOutput(Class<?> clazz, Level level) {
		this(clazz.getName(), level);
	}

	private LoggerOutput(String loggerName, Level level) {
		super("LoggerOutput:" + loggerName);
		this.level = level;
		this.logger = Instances.fetch(() -> Logger.getLogger(loggerName), Logger.class, loggerName);
		loggings = new HashMap<>();
		if (loggings.isEmpty()) {
			loggings.put(Level.TRACE, s -> logger.trace(s));
			loggings.put(Level.DEBUG, s -> logger.debug(s));
			loggings.put(Level.INFO, s -> logger.info(s));
			loggings.put(Level.WARN, s -> logger.warn(s));
			loggings.put(Level.ERROR, s -> logger.error(s));
		}
	}

	@Override
	protected boolean enqueue0(String item) {
		loggings.get(level).accept(item);
		return true;
	}
}
