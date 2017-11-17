package com.hzcominfo.dataggr.uniquery;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.NlsString;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SqlExplainer {

    public static SqlParser.ConfigBuilder DEFAULT_PARSER_CONFIG = SqlParser
            .configBuilder()
            .setParserFactory(SqlParserImpl.FACTORY)
            .setQuoting(Quoting.DOUBLE_QUOTE)
            .setUnquotedCasing(Casing.UNCHANGED)
            .setQuotedCasing(Casing.UNCHANGED)
            .setConformance(SqlConformanceEnum.DEFAULT)
//                .setConformance(SqlConformanceEnum.LENIENT) // allow everything
//            .setConformance(SqlConformanceEnum.MYSQL_5) // allow Limit start,count ,,,start is from 1
            .setCaseSensitive(true);

    public static final SqlBasicVisitor<String> visitor = new SqlBasicVisitor<String>() {
        @Override
        public String visit(SqlLiteral literal) {

//                System.out.println("literal(文本常量)===>" + literal.toString());
            return super.visit(literal);
        }

        @Override
        public String visit(SqlCall call) {
            switch (call.getKind()) {
                case SELECT:
                    SqlSelect select = (SqlSelect) call;
                    System.out.println("selectList:  " + select.getSelectList().getList().stream().map(SqlNode::toString).reduce("", (s1, s2) -> s1 + "\n" + s2));
                    System.out.println("from     " + select.getFrom());
                    if (select.hasWhere()) {
                        SqlNode where = select.getWhere();
                        System.out.println("where    " + where + " " + where.getKind() + where.getClass().getSimpleName());
                    }
                    if (null != select.getGroup())
                        System.out.println("groupBy  " + select.getGroup().getList().stream().filter(Objects::nonNull).map(SqlNode::toString).reduce("\n", (s1, s2) -> s1 + "\n" + s2));
                    System.out.println("having " + select.getHaving());
                    if (select.hasOrderBy()) System.out.println("orderBy  " + select.getOrderList());
                    System.out.println("offset " + select.getOffset());

                        /*select.getOperandList().stream().filter(Objects::nonNull).forEach(n -> {
                            System.out.println("\nselect: " + n.getKind() + "\n" + n.toString());
                        });*/
                    break;
                case AS:
                    System.out.println("AS::   " + ((SqlBasicCall) call).getOperandList());
                    break;
                case AND:
                    System.out.println("AND::   " + ((SqlBasicCall) call).getOperandList());
                    break;
                case CAST:
                    System.out.println("CAST::   " + ((SqlBasicCall) call).getOperandList());
                    break;
                default:
//                        System.out.println(call.getKind().name() + " ???? " + call.getClass().getName());

            }
//                    call.getOperandList().forEach(n -> System.out.println(n.toString()));
            return super.visit(call);
        }

        @Override
        public String visit(SqlNodeList nodeList) {
//                nodeList.forEach(n -> System.out.println("nodeList===>" + n.getClass().getName()));
            return super.visit(nodeList);
        }

        @Override
        public String visit(SqlIdentifier id) {
//                System.out.println("identifier===>" + id.getSimple());
            return super.visit(id);
        }

        @Override
        public String visit(SqlDataTypeSpec type) {

            return super.visit(type);
        }

        @Override
        public String visit(SqlDynamicParam param) {
            System.out.println("param index at " + param.getIndex());
            return super.visit(param);
        }

        @Override
        public String visit(SqlIntervalQualifier intervalQualifier) {
            return super.visit(intervalQualifier);
        }
    };

    private static Map<String, SqlNode> cache = new HashMap<>();

    public static JsonObject explain(String sql, Object... params) throws Exception {
        if (null == sql || sql.isEmpty()) return null;
        SqlNode node = cache.get(sql);
        if (null == node) {
            node = SqlParser.create(sql, DEFAULT_PARSER_CONFIG.build()).parseQuery();
            if (!node.isA(SqlKind.QUERY)) throw new Exception("Non-query expression ```" + sql + "``` is NOT supported.");
            cache.put(sql, node);
        }

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
        return json;
    }

    public static String explainAsJsonString(String sql, Object... params) throws Exception {
        return explain(sql, params).toString();
    }

    private static void analysisSqlSelect(SqlSelect select, JsonObject json) {
        //keywordList
        analysisKeywords(select, json);
        //selectList
        analysisFields(select.getSelectList().getList(), json);
        //from ok
        analysisTables(select, json);
        //where //todo not finished
        analysisConditions(select.getWhere(), json);
        //groupBy
        analysisGroupBy(select.getGroup(), json);
        //having
        analysisHaving(select.getHaving(), json);
        //order
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
            SqlIdentifier identifier;
            JsonObject field = new JsonObject();
            switch (n.getKind()) {
                case AS:
                    identifier = ((SqlCall) n).operand(0);
                    identifier2Json(identifier, field);
                    field.addProperty("alias", ((SqlIdentifier) ((SqlCall) n).operand(1)).getSimple());
                    break;
                case IDENTIFIER:
                    identifier = (SqlIdentifier) n;
                    identifier2Json(identifier, field);
//                    field.addProperty("field", identifier.isStar() ? "*" : identifier.getSimple());
                    break;
                case SUM:
                case AVG:
                case COUNT:
                case MAX:
                case MIN:
                default:
                    throw new RuntimeException("Unsupported kind: " + n.getKind());
            }
            fields.add(field);
        }
        json.add("fields", fields);
    }

    private static void identifier2Json(SqlIdentifier identifier, JsonObject json) {
        ImmutableList<String> names = identifier.names.reverse();
//        if (names.size() >= 3) json.addProperty("namespace", names.get(2));
//        if (names.size() >= 2) json.addProperty("table", names.get(1));
//        if (names.size() >= 1) json.addProperty("field", identifier.isStar() ? "*" : names.get(0));
        json.addProperty("field", identifier.toString());
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
//        System.out.println("\n" + condition);
        if (null == condition) return;
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
            case LIKE:
//            case NOT:
                JsonObject o = new JsonObject();
                analysisBinaryConditionExpression(sc, o);
                tree.add(kind.lowerName, o);
                break;
            case BETWEEN:
                break;
            case IN:
//                JsonObject ij =
            case NOT_IN:
                analysisMultipleConditionExpression(sc, tree);
//                throw new RuntimeException("Condition " + kind.lowerName + " NOT supported");
                break;
            default:
                throw new RuntimeException("Unsupported Condition kind: " + kind.name());
        }
    }

    /**
     * 复合表达式解析
     * @param sc SqlCall
     * @param array result
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
     * @param sc SqlCall
     * @param object result
     */
    private static void analysisUnaryConditionExpression(SqlCall sc, JsonObject object) {
        String snn = ((SqlIdentifier) sc.operand(0)).getSimple();
        JsonObject nnj = new JsonObject();
        nnj.add(snn, null);
        object.add(sc.getKind() == SqlKind.IS_NULL ? SqlKind.EQUALS.lowerName : SqlKind.NOT_EQUALS.lowerName, nnj);
    }

    /**
     * 二元表达式解析
     * @param sc SqlCall
     * @param json result
     */
    private static void analysisBinaryConditionExpression(SqlCall sc, JsonObject json) {
        String field = ((SqlIdentifier) sc.operand(0)).getSimple();
        SqlNode v = sc.operand(1);
        if (v instanceof SqlIdentifier) {
            throw new RuntimeException("if " + ((SqlIdentifier) v).getSimple() + " (" + v.getParserPosition().toString() + ") is literal, please mark them by single quote");
        }
//        Object value = ((SqlLiteral) sc.operand(1)).getValue();
        //todo 这种写法导致不支持浮点数, 还有一处类似的
        Object value = ((SqlLiteral) sc.operand(1)).getValue();
        if (value instanceof NlsString) {
            json.addProperty(field, ((NlsString) value).getValue());
        } else if (value instanceof BigDecimal) {
            json.addProperty(field, ((BigDecimal) value).intValue());
        } else {
            System.out.println("===============WRONG===================");
        }
    }

    /**
     * 多元表达式解析
     * @param sc SqlCall
     * @param json result
     */
    private static void analysisMultipleConditionExpression(SqlCall sc, JsonObject json) {
        System.out.println(sc);
//        String operator = sc.getOperator().getName();
        String operator = sc.getKind().lowerName;
        String field = ((SqlIdentifier) sc.operand(0)).getSimple();
        SqlNode nodes = sc.operand(1);
        JsonObject object = new JsonObject();
        JsonArray array = new JsonArray();
        object.add(field, array);
        json.add(operator, object);
        for (SqlNode node : ((SqlNodeList) nodes).getList()) {
            if (!( node instanceof SqlLiteral)) throw new RuntimeException(node + " is NOT a literal");
            Object value = ((SqlLiteral) node).getValue();
            if (value instanceof BigDecimal) array.add(((BigDecimal) value).longValue());
            else if (value instanceof NlsString) array.add(((NlsString) value).getValue());
        }
    }

    private static void analysisGroupBy(SqlNodeList groupList, JsonObject json) {
        JsonArray array = new JsonArray();
        json.add("groupBy", array);
        if (null == groupList) return;
        groupList.getList().forEach(node -> {
            if (node.getKind() == SqlKind.IDENTIFIER) {
                array.add(((SqlIdentifier) node).getSimple());
            } else {
                System.out.println("Unsupported GROUP kind " + node.getKind().name());
            }
        });
    }

    private static void analysisHaving(SqlNode having, JsonObject json) {
        JsonObject array = new JsonObject();
        json.add("having", array);
        if (null == having) return;
        System.out.println(having);
        throw new RuntimeException(having.getKind().name() + " is NOT supported");
    }

    private static void analysisOrderBy(SqlNodeList orderList, JsonObject json) {
        JsonArray array = new JsonArray();
        json.add("orderBy", array);
        if (null == orderList) return;
        for (SqlNode node : orderList.getList()) {
            JsonObject object = new JsonObject();
            if (node instanceof SqlIdentifier) {
                object.addProperty(((SqlIdentifier) node).getSimple(), "ASC");
                array.add(object);
            } else if (node instanceof SqlBasicCall) {
                SqlBasicCall sbc = (SqlBasicCall) node;
                SqlIdentifier identifier = sbc.operand(0);
                object.addProperty(identifier.getSimple(), sbc.getOperator().getName());
                array.add(object);
            } else {
                System.out.println("Error to parse ORDER BY from " + node);
            }
        }
    }

    private static void analysisOffset(SqlNode node, JsonObject json) {
        long offset = 0L;
        if (node instanceof SqlNumericLiteral) {
            offset = ((SqlNumericLiteral) node).bigDecimalValue().longValue();
            if (offset < 0) throw new RuntimeException(node + " analysis failed, Maybe it is larger than Long.MAX_VALUE");
        }
        json.addProperty("offset", offset);
    }

    private static void analysisLimit(SqlNode node, JsonObject json) {
        long limit = -1L;
        if (node instanceof SqlNumericLiteral) {
            limit = ((SqlNumericLiteral) node).bigDecimalValue().longValue();
            if (limit < 0) throw new RuntimeException(node + " analysis failed, Maybe it is larger than Long.MAX_VALUE");
//            limit = ((SqlNumericLiteral) node).intValue(false);
        }
        json.addProperty("limit", limit);
    }



}



