package com.hzcominfo.dataggr.uniquery;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.NlsString;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hzcominfo.dataggr.uniquery.utils.ExceptionUtil;

import net.butfly.albacore.utils.logger.Logger;

public class SqlExplainer {
	private static final Logger logger = Logger.getLogger(SqlExplainer.class);
	public static final String KEYWORD_FUNCTION_STRING_NAME = "keyword";
	public static final int LIMIT_DEFAULT;
	public static SqlParser.ConfigBuilder DEFAULT_PARSER_CONFIG = SqlParser.configBuilder()
			.setParserFactory(SqlParserImpl.FACTORY).setQuoting(Quoting.DOUBLE_QUOTE)
			.setUnquotedCasing(Casing.UNCHANGED).setQuotedCasing(Casing.UNCHANGED)
			.setConformance(SqlConformanceEnum.DEFAULT).setConformance(SqlConformanceEnum.LENIENT) // allow
																									// everything
			// .setConformance(SqlConformanceEnum.MYSQL_5) // 默认sql标准为mysql_5
			// allow Limit start,count ,,,start is from 1
			.setCaseSensitive(true);

	private static Map<String, JsonObject> explains = new ConcurrentHashMap<>();
	private static final AtomicInteger DYNAMIC_PARAM_INDEX = new AtomicInteger(0);

	static {
		String strValue = System.getProperty("uniquery.default.limit");
		if (null == strValue) {
			LIMIT_DEFAULT = 10000;
			logger.debug("system property 'uniquery.default.limit' not assigned, use default value: " + LIMIT_DEFAULT);
		} else {
			int limit = 10000;
			try {
				limit = Integer.valueOf(strValue);
			} catch (NumberFormatException e) {
				logger.warn("can NOT parse default limit as INT from system property 'uniquery.default.limit="
						+ strValue + "', use default value: " + limit);
			}
			LIMIT_DEFAULT = limit;
		}
	}

	public static JsonObject explain(String sql, Object... params) {
		JsonObject json = explains.compute(sql, (s, e) -> null == e ? newExplain(s, params) : e);
		return replaceJsonDynamicParams(json, params);
	}

	private static JsonObject newExplain(String sql, Object... params) {
		if (null == sql || sql.isEmpty())
			return null;
		SqlNode node = null;
		try {
			node = SqlParser.create(sql, DEFAULT_PARSER_CONFIG.build()).parseQuery();
		} catch (SqlParseException e1) {
			ExceptionUtil.runtime("sql parse error: ", e1);
		}
		if (!node.isA(SqlKind.QUERY))
			throw new RuntimeException("Non-query expression ```" + sql + "``` is NOT supported.");

		JsonObject json = new JsonObject();
		json.addProperty("sql", sql);

		switch (node.getKind()) {
		case SELECT:
			SqlSelect select = (SqlSelect) node;
			analysisSqlSelect(select, json);
			break;
		case ORDER_BY:
			SqlOrderBy orderBy = (SqlOrderBy) node;
			SqlNode query = orderBy.query;
			analysisSqlSelect((SqlSelect) query, json);
			SqlNode fetch = orderBy.fetch;
			analysisLimit(fetch, json);
			SqlNode offset = orderBy.offset;
			analysisOffset(offset, json);
			SqlNodeList orderList = orderBy.orderList;
			analysisOrderBy(orderList, json);
			break;
		case UNION:
		case INTERSECT:
		case EXCEPT:
		case VALUES:
		case WITH:
		case EXPLICIT_TABLE:
		default:
		}

		json.addProperty("dynamic_param_size", DYNAMIC_PARAM_INDEX.get());
		DYNAMIC_PARAM_INDEX.set(0); // reset
		return json;
	}

	private static String newDynamicParamMark() {
		return "$${" + DYNAMIC_PARAM_INDEX.incrementAndGet() + "}";
	}

	public static String explainAsJsonString(String sql, Object... params) throws Exception {
		return explain(sql, params).toString();
	}

	private static void analysisSqlSelect(SqlSelect select, JsonObject json) {
		// keywordList
		analysisKeywords(select, json);
		// selectList
		analysisFields(select.getSelectList().getList(), json);
		// from ok
		analysisTables(select, json);
		// where //todo not finished
		analysisConditions(select.getWhere(), json);
		// groupBy
		analysisGroupBy(select.getGroup(), json);
		// having
		analysisHaving(select.getHaving(), json);
		// order
		analysisOrderBy(select.getOrderList(), json);
		// offset
		analysisOffset(select.getOffset(), json);
		// limit
		analysisLimit(select.getFetch(), json);
	}

	private static void analysisKeywords(SqlSelect select, JsonObject json) {
		json.addProperty("distinct", select.isDistinct());
	}

	private static void analysisFields(List<SqlNode> selectList, JsonObject json) {
		JsonArray fields = new JsonArray();
		for (SqlNode n : selectList) {
			JsonObject field = new JsonObject();
			switch (n.getKind()) {
			case AS:
				SqlNode nd = ((SqlCall) n).operand(0);
				String names = SqlDynamicParam.class.isInstance(nd) ? newDynamicParamMark() : fieldNameFromSqlNode(nd);
				field.addProperty("field", names);
				field.addProperty("alias", sqlNodeNames(((SqlCall) n).operand(1)));
				break;
			case IDENTIFIER:
				String identifierNames = sqlNodeNames((SqlIdentifier) n);
				field.addProperty("field", identifierNames);
				field.addProperty("alias", identifierNames);
				// field.addProperty("field", identifier.isStar() ? "*" :
				// identifier.getSimple());
				break;
			case DYNAMIC_PARAM:
				field.addProperty("field", newDynamicParamMark());
				// throw new RuntimeException("未指定 AS!");
				break;
			case SUM:
			case SUM0:
			case AVG:
			case COUNT:
			case MAX:
			case MIN:
				field.addProperty("field", fieldNameFromSqlNode(n));
				field.addProperty("alias", fieldNameFromSqlNode(n));
				break;
			case OTHER_FUNCTION:
				field.addProperty("field", fieldNameFromSqlNode(n));
				field.addProperty("alias", sqlNodeNames(((SqlCall) n).operand(0)));
				break;
			default:
				throw new RuntimeException("Unsupported kind: " + n.getKind());
			}
			fields.add(field);
		}
		json.add("fields", fields);
	}

	private static void analysisTables(SqlSelect select, JsonObject json) {
		SqlNode from = select.getFrom();
		JsonObject tree = new JsonObject();
		analysisWithJoin(from, tree);
		json.add("tables", tree);
	}

	private static void analysisWithJoin(SqlNode join, JsonObject tree) {
		switch (join.getKind()) {
		case IDENTIFIER:
			SqlIdentifier identifier = (SqlIdentifier) join;
			tree.addProperty("table", identifier.toString());
			break;
		case AS:
			tree.addProperty("table", ((SqlIdentifier) ((SqlCall) join).operand(0)).toString());
			tree.addProperty("alias", ((SqlIdentifier) ((SqlCall) join).operand(1)).getSimple());
			break;
		case JOIN:
			JsonObject left = new JsonObject(), right = new JsonObject();
			SqlJoin sqlJoin = (SqlJoin) join;
			analysisWithJoin(sqlJoin.getLeft(), left);
			analysisWithJoin(sqlJoin.getRight(), right);
			tree.addProperty("type", sqlJoin.getJoinType().lowerName);
			tree.add("left", left);
			tree.add("right", right);
			break;
		default:
			throw new RuntimeException("Unsupported from kind: " + join.getKind());
		}
	}

	private static void analysisConditions(SqlNode where, JsonObject json) {
		JsonObject tree = new JsonObject();
		analysisConditionExpression(where, tree);
		json.add("where", tree);
	}

	private static void analysisConditionExpression(SqlNode condition, JsonObject tree) {
		if (null == condition)
			return;
		SqlKind kind = condition.getKind();
		SqlCall sc = (SqlCall) condition;

		switch (kind) {
		case AND:
		case OR:
			JsonArray array = new JsonArray();
			analysisCompoundConditionExpression(sc, array);
			tree.add(kind.lowerName, array);
			break;
		case IS_NULL:
		case IS_NOT_NULL:
			analysisUnaryConditionExpression(sc, tree);
			break;
		case GREATER_THAN:
		case LESS_THAN:
		case GREATER_THAN_OR_EQUAL:
		case LESS_THAN_OR_EQUAL:
		case EQUALS:
		case NOT_EQUALS:
			JsonObject o = new JsonObject();
			analysisBinaryConditionExpression(sc, o);
			tree.add(kind.lowerName, o);
			break;
		case LIKE:
			JsonObject j = new JsonObject();
			analysisBinaryConditionExpression(sc, j);
			tree.add(
					(sc.getOperator().getName().toLowerCase().startsWith("not") ? "not_" : "") + sc.getKind().lowerName,
					j); // for support not like
			break;
		case BETWEEN:
			analysisTernaryConditionExpression(sc, tree);
			break;
		case IN:
		case NOT_IN:
			analysisMultipleConditionExpression(sc, tree);
			break;
		// agg func can use in `having` but can't use in `where`
		case SUM:
		case SUM0:
		case AVG:
		case COUNT:
		case MAX:
		case MIN:
			break;
		case OTHER_FUNCTION:
			analysisGeoDistanceConditionExpression(sc, tree);
			break;
		default:
			throw new RuntimeException("Unsupported Condition kind: " + kind.name());
		}
	}

	private static String GEO_DISTANCE = "geo_distance";
	private static int GEO_DISTANCE_PARAMS_NUM = 4;
	private static String[] GEO_DISTANCE_PARAMS = { "field", "lat", "lon", "d" };
	
	private static void analysisGeoDistanceConditionExpression(SqlCall sc, JsonObject tree) {
		String func = sc.getOperator().getName().toLowerCase();
		if (GEO_DISTANCE.equals(func)) {
			if (sc.operandCount() == GEO_DISTANCE_PARAMS_NUM) {
				JsonObject json = new JsonObject();
				for (int i = 0; i < GEO_DISTANCE_PARAMS_NUM; i++) {
					SqlNode node = sc.operand(i);
					switch (node.getKind()) {
					case IDENTIFIER:
						json.addProperty(GEO_DISTANCE_PARAMS[i], ((SqlIdentifier) node).toString());
						break;
					case LITERAL:
						json.addProperty(GEO_DISTANCE_PARAMS[i], ((SqlLiteral) node).bigDecimalValue());
						break;
					case DYNAMIC_PARAM:
						json.addProperty(GEO_DISTANCE_PARAMS[i], newDynamicParamMark());
						break;
					default:
						json.addProperty(GEO_DISTANCE_PARAMS[i], node.toString());
						break;
					}
				}
				tree.add(func, json);
			} else
				throw new RuntimeException("Please check your params when using geo_distance(field,lat,lon,d)!");
		}
	}

	/**
	 * 复合表达式解析
	 * 
	 * @param sc
	 *            SqlCall
	 * @param array
	 *            result
	 */
	private static void analysisCompoundConditionExpression(SqlCall sc, JsonArray array) {
		JsonObject lj = new JsonObject(), rj = new JsonObject();
		SqlNode ln = sc.operand(0), rn = sc.operand(1);
		analysisConditionExpression(ln, lj);
		analysisConditionExpression(rn, rj);
		array.add(lj);
		array.add(rj);
	}

	/**
	 * 一元表达式解析
	 * 
	 * @param sc
	 *            SqlCall
	 * @param object
	 *            result
	 */
	private static void analysisUnaryConditionExpression(SqlCall sc, JsonObject object) {
		String value = null;
		SqlNode node = sc.operand(0);
		if (node instanceof SqlDynamicParam)
			value = newDynamicParamMark();
		else
			value = fieldNameFromSqlNode(node);
		object.addProperty(sc.getKind().lowerName, value);
	}

	/**
	 * 二元表达式解析
	 * 
	 * @param sc
	 *            SqlCall
	 * @param json
	 *            result
	 */
	private static void analysisBinaryConditionExpression(SqlCall sc, JsonObject json) {
		SqlNode node;
		node = sc.operand(0);
		String field = fieldNameFromSqlNode(node);
		node = sc.operand(1);
		if (node.getKind() == SqlKind.IDENTIFIER) {
			json.addProperty(field, sqlNodeNames(((SqlIdentifier) node)));
			return;
		} else if (node.getKind() == SqlKind.DYNAMIC_PARAM) {
			json.addProperty(field, newDynamicParamMark());
			return;
		}
		// todo 这种写法导致不支持浮点数, 还有一处类似的
		Object value = ((SqlLiteral) node).getValue();
		if (value instanceof NlsString) {
			json.addProperty(field, ((NlsString) value).getValue());
		} else if (value instanceof BigDecimal) {
			json.addProperty(field, ((BigDecimal) value).intValue());
		} else {
			System.out.println("===============WRONG===================");
		}
	}

	/**
	 * 三元表达式解析 目前只解析 BETWEEN
	 * 
	 * @param sc
	 *            SqlCall
	 * @param json
	 *            result
	 */
	private static void analysisTernaryConditionExpression(SqlCall sc, JsonObject json) {
		List<SqlNode> operands = sc.getOperandList();
		SqlNode node;
		node = operands.get(0);
		String field = (node instanceof SqlDynamicParam) ? newDynamicParamMark() : fieldNameFromSqlNode(node);
		node = operands.get(1);
		String start = ((node instanceof SqlDynamicParam) ? newDynamicParamMark() : ((SqlLiteral) node).toValue());
		node = operands.get(2);
		String end = ((node instanceof SqlDynamicParam) ? newDynamicParamMark() : ((SqlLiteral) node).toValue());
		JsonArray array = new JsonArray();
		array.add(start);
		array.add(end);
		JsonObject object = new JsonObject();
		object.add(field, array);
		json.add((sc.getOperator().getName().toLowerCase().startsWith("not") ? "not_" : "") + sc.getKind().lowerName,
				object);
	}

	/**
	 * 多元表达式解析
	 * 
	 * @param sc
	 *            SqlCall
	 * @param json
	 *            result
	 */
	private static void analysisMultipleConditionExpression(SqlCall sc, JsonObject json) {
		// String operator = sc.getOperator().getName();
		String operator = sc.getKind().lowerName;
		String field;
		if (sc.operand(0) instanceof SqlDynamicParam)
			field = newDynamicParamMark();
		else
			field = fieldNameFromSqlNode(sc.operand(0));

		SqlNode nodes = sc.operand(1);
		JsonObject object = new JsonObject();
		JsonArray array = new JsonArray();
		object.add(field, array);
		json.add(operator, object);
		for (SqlNode node : ((SqlNodeList) nodes).getList()) {
			if (node instanceof SqlDynamicParam) {
				array.add(newDynamicParamMark());
			} else {
				if (!(node instanceof SqlLiteral))
					throw new RuntimeException(node + " is NOT a literal");
				Object value = ((SqlLiteral) node).getValue();
				if (value instanceof BigDecimal)
					array.add(((BigDecimal) value).longValue());
				else if (value instanceof NlsString)
					array.add(((NlsString) value).getValue());
			}
		}
	}

	private static void analysisGroupBy(SqlNodeList groupList, JsonObject json) {
		JsonArray array = new JsonArray();
		json.add("groupBy", array);
		if (null == groupList)
			return;
		groupList.getList().forEach(node -> {
			String field = fieldNameFromSqlNode(node);
			array.add(field);
			/*
			 * if (node.getKind() == SqlKind.IDENTIFIER) {
			 * array.add(((SqlIdentifier)
			 * node).names.stream().collect(Collectors.joining("."))); } else {
			 * System.out.println("Unsupported GROUP kind " +
			 * node.getKind().name()); }
			 */
		});
	}

	private static void analysisHaving(SqlNode having, JsonObject json) {
		if (null == having)
			return;
		System.out.println(having);
		JsonObject object = new JsonObject();
		analysisConditionExpression(having, object);
		json.add("having", object);
	}

	private static void analysisOrderBy(SqlNodeList orderList, JsonObject json) {
		JsonArray array = new JsonArray();
		json.add("orderBy", array);
		if (null == orderList)
			return;
		for (SqlNode node : orderList.getList()) {
			JsonObject object = new JsonObject();
			if (node instanceof SqlIdentifier) { // ASC
				object.addProperty(sqlNodeNames((SqlIdentifier) node), "ASC");
				array.add(object);
			} else if (node instanceof SqlBasicCall) {
				String name;
				if (SqlPostfixOperator.class.isInstance(((SqlBasicCall) node).getOperator())) {
					name = fieldNameFromSqlNode(((SqlBasicCall) node).operand(0));
					object.addProperty(name, "DESC");
				} else {
					name = fieldNameFromSqlNode(node);
					object.addProperty(name, "ASC");
				}
				array.add(object);
			} else {
				System.out.println("Error to parse ORDER BY from " + node);
			}
		}
	}

	private static String sqlNodeNames(SqlNode node) {
		switch (node.getKind()) {
		case IDENTIFIER:
			return ((SqlIdentifier) node).toString();
		case LITERAL:
			return ((SqlLiteral) node).toValue();
		default:
			return node.toString();
		}
		// return identifier.toString();
		// if (identifier.isStar()) return identifier.toString();
		// return identifier.names.stream().collect(Collectors.joining("."));
	}

	private static String fieldNameFromSqlNode(SqlNode node) {
		if (SqlIdentifier.class.isInstance(node))
			return sqlNodeNames((SqlIdentifier) node);
		else if (SqlBasicCall.class.isInstance(node)) {
			SqlOperator operator = ((SqlBasicCall) node).getOperator();
			String identifier = sqlNodeNames(((SqlBasicCall) node).operand(0));
			if (SqlFunction.class.isInstance(operator)) {
				String function = operator.getName();
				return KEYWORD_FUNCTION_STRING_NAME.equals(function) ? identifier + "." + KEYWORD_FUNCTION_STRING_NAME
						: function + "(" + identifier + ")";
			} else
				throw new RuntimeException("unsupported sql operator:" + operator);
		} else
			throw new RuntimeException("cant not get field name from sql node: " + node);
	}

	private static void analysisOffset(SqlNode node, JsonObject json) {
		long offset = 0L;
		if (node instanceof SqlNumericLiteral) {
			offset = ((SqlNumericLiteral) node).bigDecimalValue().longValue();
			if (offset < 0)
				throw new RuntimeException(node + " analysis failed, Maybe it is larger than Long.MAX_VALUE");
		}
		json.addProperty("offset", offset);
	}

	private static void analysisLimit(SqlNode node, JsonObject json) {
		long limit = LIMIT_DEFAULT;
		if (node instanceof SqlNumericLiteral) {
			limit = ((SqlNumericLiteral) node).bigDecimalValue().longValue();
			if (limit < 0)
				throw new RuntimeException(node + " analysis failed, Maybe it is larger than Long.MAX_VALUE");
			// limit = ((SqlNumericLiteral) node).intValue(false);
		}
		json.addProperty("limit", limit);
	}

	private static JsonObject replaceJsonDynamicParams(JsonObject json, Object... params) {
		assert null != json;
		int paramSize = json.get("dynamic_param_size").getAsInt();
		if (0 == paramSize)
			return json;
		if (null == params || params.length != paramSize)
			ExceptionUtil.runtime(
					"dynamic param Size: " + paramSize + ", params Size: " + (null == params ? null : params.length));
		Pattern pattern = Pattern.compile("\\$\\$\\{\\d+}");
		String source = json.toString();
		Matcher matcher = pattern.matcher(source);
		while (matcher.find()) {
			String group = matcher.group();
			int index = Integer.valueOf(group.substring(0, group.length() - 1).substring(3));
			source = source.replace(group, params[index - 1].toString());
		}
		return new JsonParser().parse(source).getAsJsonObject();
	}

}
