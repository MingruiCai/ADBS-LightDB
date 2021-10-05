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
 * judge whether the join condition meet in the tuple
 */
public class JoinVisitor extends ExpressionDeParser {
    public Tuple leftTuple;
    public Tuple rightTuple;
    public Table leftTable;
    public Table rightTable;
    public Expression expression;
    public HashMap<String, Table> dataCatalog;

    /**
     * receive all required parameters
     * @param leftTuple
     * @param rightTuple
     * @param leftTable
     * @param rightTable
     * @param expression WHERE expression
     * @param dataCatalog data structure read from the scheme
     * @throws JSQLParserException
     */
    public JoinVisitor(Tuple leftTuple, Tuple rightTuple, Table leftTable, Table rightTable, Expression expression, HashMap<String, Table> dataCatalog) throws JSQLParserException {
        this.leftTuple = leftTuple;
        this.rightTuple = rightTuple;
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.expression = expression;
        this.dataCatalog = dataCatalog;
//        evaluate(expression.toString());
    }

    /**
     * implement all the required operations(=,>,<....)
     * the main idea is to push both left and rigth column to the stack with correspoding signs(=,<...)
     * It will check whether true or false
     * @param expr the WHERE expression.toString()
     * @return whether the condition is true or false in this tuple
     * @throws JSQLParserException
     */
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

            /**
             * this visit method is quite special
             * it will find the attributes of the tables on both side
             * so it can get the value of the left side and right side
             * and push two integers to the stack
             * @param tableColumn
             */
            @Override
            public void visit(Column tableColumn) {
                super.visit(tableColumn);
                String columnName = tableColumn.getColumnName();
                String tableName = tableColumn.getTable().toString();
                Table table = dataCatalog.get(tableName);
                for (int i = 0; i < table.attributes.length; i++) {
                    if (columnName.equals(table.attributes[i])) {
                        if (table.name == leftTable.name) {
                            stack.push((long) leftTuple.attributes[i]);
                        } else if (table.name == rightTable.name) {

                            stack.push((long) rightTuple.attributes[i]);

                        }
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
