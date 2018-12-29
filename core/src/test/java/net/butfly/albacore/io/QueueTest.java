package net.butfly.albacore.io;

import org.slf4j.event.Level;

import net.butfly.albatis.io.ext.LoggerOutput;
import net.butfly.albatis.io.ext.RandomStringInput;
import net.butfly.albatis.io.pump.Pump;

public class QueueTest {
	public static void main(String... args) {
		try (Pump<String> p = RandomStringInput.INSTANCE.pump(3, new LoggerOutput(Level.WARN))) {
			p.open();
		}
	}
}
