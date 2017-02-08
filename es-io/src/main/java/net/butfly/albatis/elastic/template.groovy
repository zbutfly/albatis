if (ctx._source.{0} == null) ctx._source.{0} = [];
else if (!([].class.class.isInstance(ctx._source.{0}))) ctx._source.{0} = [ctx._source.{0}].flatten().findAll '{' it != null '}';
ctx._source.{0}.removeAll '{' it.{1} == values.{1} '}';
if (ctx._source.{0} == null) ctx._source.{0} = [];
else if (!([].class.class.isInstance(ctx._source.{0}))) ctx._source.{0} = [ctx._source.{0}].flatten().findAll '{' it != null '}';
ctx._source.{0}.add(values);
