package net.butfly.albatis.io.format;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import net.butfly.albatis.io.Rmap;
import net.butfly.alserder.SerDes.ListSerDes;

public interface Format extends ListSerDes<Rmap, Rmap> {
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	@interface FormatAs { // default value for bson
		String value();
	}
}
