package net.butfly.albatis.dao;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Date;
import java.util.List;

import net.butfly.albacore.exception.ConfigException;
import net.butfly.albatis.kafka.KafkaInput;
import net.butfly.albatis.kafka.KafkaMessage;

public class KafkaTest {
	public static void main(String[] args) throws ConfigException, IOException {
		// topics.put("HZGA_GAZHK_LGY_NB", 3);
		NumberFormat nf = new DecimalFormat("0.00");
		long total = 0;
		long begin = new Date().getTime();
		long now = begin;
		try (KafkaInput in = new KafkaInput("KafkaInput", "kafka.properties", "HZGA_GAZHK_HZKK");) {
			long size = 0;
			do {
				List<KafkaMessage> l = in.dequeue(10000);
				for (KafkaMessage m : l)
					size += m.toBytes().length;
				long curr = new Date().getTime();
				total += (size / 1024.0 / 1024);
				System.out.println("time of [" + l.size() + "]: <" + nf.format((curr - now) / 1000.0) + " secs>, "//
						+ "size: <" + nf.format(size / 1024.0 / 1024) + " MByte>, "//
						+ "throughtout: <" + nf.format((size / 1024.0 / 1024) / ((curr - now) / 1000.0)) + " MBytes/sec>, "//
						+ "total:  <" + nf.format(total/1024) + " Gbytes>, "//
						+ "avg: <" + nf.format(total / ((curr - begin) / 1000.0)) + "> MBytes/sec.");
				size = 0;
				now = new Date().getTime();
			} while (true);
		}
	}
}
