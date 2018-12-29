package net.butfly.albatis.io.pump;

//TODO
public class FanOutPump<V> extends PumpImpl<V, FanOutPump<V>> {
	protected FanOutPump(String name, int parallelism) {
		super(name, parallelism);
	}
}
