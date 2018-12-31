package net.butfly.albatis.io;

import static net.butfly.albatis.ddl.TableDesc.dummy;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import net.butfly.albatis.ddl.TableDesc;

public interface IOFactory extends Formatable {
	default <M extends Rmap> Input<M> input(TableDesc... table) throws IOException {
		return input(Arrays.asList(table), formats());
	}

	default <M extends Rmap> Input<M> input(String... table) throws IOException {
		return input(dummy(table), formats());
	}

	default <M extends Rmap> Input<M> input(Map<String, String> keyMapping) throws IOException {
		return input(dummy(keyMapping), formats());
	}

	default <M extends Rmap> Output<M> output() throws IOException {
		return output(Arrays.asList(), formats());
	}

	default <M extends Rmap> Output<M> output(TableDesc... table) throws IOException {
		return output(Arrays.asList(table), formats());
	}

	default <M extends Rmap> Output<M> output(String... table) throws IOException {
		return output(dummy(table), formats());
	}

	default <M extends Rmap> Output<M> output(Map<String, String> keyMapping) throws IOException {
		return output(dummy(keyMapping), formats());
	}
}
