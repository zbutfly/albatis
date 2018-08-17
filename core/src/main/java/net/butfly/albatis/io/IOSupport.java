package net.butfly.albatis.io;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import net.butfly.albatis.ddl.FieldDesc;

public interface IOSupport {
	default <M extends Rmap> Input<M> input(String... table) throws IOException {
		return input(Arrays.asList(table));
	}

	default <M extends Rmap> Input<M> input(Collection<String> tables) throws IOException {
		return input(tables, new FieldDesc[0]);
	}

	default <M extends Rmap> Input<M> input(Collection<String> tables, FieldDesc... fields) throws IOException {
		Input<M> i = input(tables.toArray(new String[tables.size()]));
		if (null != fields && fields.length > 0) i.schema(fields);
		return i;
	}

	default <M extends Rmap> Output<M> output(String... table) throws IOException {
		return output(Arrays.asList(table));
	}

	default <M extends Rmap> Output<M> output(Collection<String> tables) throws IOException {
		return output(tables, new FieldDesc[0]);
	}

	default <M extends Rmap> Output<M> output(Collection<String> tables, FieldDesc... fields) throws IOException {
		Output<M> o = output(tables.toArray(new String[tables.size()]));
		if (null != fields && fields.length > 0) o.schema(fields);
		return o;
	}
}
