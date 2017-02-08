if (ctx._source.{0}.length === undefined) '{'
	ctx._source.{0} = [ctx._source.{0}];
'}'
for ( ; i < ctx._source.{0}.length; i++) '{'
	if(ctx._source.{0}[i].{1} === values.{1}) '{'
		ctx._source.{0}[i] = values;
		break;
	'}'
'}'
if (i >= ctx._source.{0}.size()) '{'
	ctx._source.{0}.add(values);
'}'
