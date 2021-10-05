package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;

import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.HashMap;
import java.util.Stack;

/**
 * Similar to other visitor class
 * check true or false for the given tuple according to the given expression
 */
public class Visitor extends ExpressionDeParser {
    public Tuple tuple;
    public Expression expression;
    public HashMap<String, Table> dataCatalog;

    /**
     * receive parameters
     * @param tuple
     * @param expression
     * @param dataCatalog
     * @throws JSQLParserException
     */
    public Visitor(Tuple tuple, Expression expression, HashMap<String, Table> dataCatalog) throws JSQLParserException {
        this.tuple = tuple;
        this.expression = expression;
        this.dataCatalog=dataCatalog;
//        evaluate(expression.toString());
    }

    public Boolean evaluate(String expr) throws JSQLParserException {
        final Stack<Long> stack = new Stack<Long>();
        final Stack<Boolean> bstack = new Stack<Boolean>();
        Expression parseExpression = CCJSqlParserUtil.parseCondExpression(expr);

        ExpressionDeParser deparser = new ExpressionDeParser() {
            @Override
            public void visit(AndExpression andExpression) {
                super.visit(andExpression);
                Boolean and1 = bstack.pop();
                Boolean and2 = bstack.pop();
                bstack.push(and1 && and2);

            }

            @Override
            public void visit(Column tableColumn) {
                super.visit(tableColumn);
                String columnName = tableColumn.getColumnName();
                String tableName = tableColumn.getTable().toString();
                Table table = dataCatalog.get(tableName);
                for (int i = 0; i < table.attributes.length; i++) {
                    if (columnName.equalsIgnoreCase(table.attributes[i])) {
                        stack.push((long) tuple.attributes[i]);
                    }
                }
            }


            @Override
            public void visit(LongValue longValue) {
                super.visit(longValue);
                stack.push(longValue.getValue());
            }

            @Override
            public void visit(EqualsTo equalsTo) {
                super.visit(equalsTo);
                long eq1 = stack.pop();
                long eq2 = stack.pop();
                bstack.push(eq1 == eq2);

            }

            @Override
            public void visit(NotEqualsTo notEqualsTo) {
                super.visit(notEqualsTo);
                long neq1 = stack.pop();
                long neq2 = stack.pop();
                bstack.push(neq1 != neq2);
            }

            @Override
            public void visit(GreaterThan greaterThan) {
                super.visit(greaterThan);
                long right = stack.pop();
                long left = stack.pop();
                bstack.push(left > right);
            }

            @Override
            public void visit(GreaterThanEquals greaterThanEquals) {
                super.visit(greaterThanEquals);
                long right = stack.pop();
                long left = stack.pop();
                bstack.push(left >= right);
            }

            @Override
            public void visit(MinorThan minorThan) {
                super.visit(minorThan);
                long right = stack.pop();
                long left = stack.pop();
                bstack.push(left < right);
            }

            @Override
            public void visit(MinorThanEquals minorThanEquals) {
                super.visit(minorThanEquals);
                long right = stack.pop();
                long left = stack.pop();
                bstack.push(left <= right);
            }

        };
        StringBuilder b = new StringBuilder();
        deparser.setBuffer(b);
        parseExpression.accept(deparser);

//        System.out.println(expr + " is " + bstack.pop());
        return bstack.pop();
    }

}
