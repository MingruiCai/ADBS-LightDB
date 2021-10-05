package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import java.util.Stack;

/**
 * split the WHERE expression in order to split join condition and select condition
 */
public class SplitWhereVisitor {
    public Stack SplitWhere_evaluate(String expr) throws JSQLParserException {


        final Stack<String> expression_item_stack = new Stack<>();
        final Stack<String> boolean_operator_stack = new Stack<>();
        final Stack<String> expression_stack = new Stack<>();
        System.out.println("WHERE_expression= " + expr);
        Expression parseExpression = CCJSqlParserUtil.parseCondExpression(expr);
        /**
         * Overload the visit() methods to splie WHERE expression
         */
        ExpressionDeParser expressionDeParser = new ExpressionDeParser() {
            @Override
            public void visit(AndExpression andExpression) {
                super.visit(andExpression);
                while (boolean_operator_stack.size() != 0) {
                    String right_item = expression_item_stack.pop();
                    String left_item = expression_item_stack.pop();
                    String expression = left_item + " " + boolean_operator_stack.pop() + " " + right_item;
                    expression_stack.push(expression);
                }
            }
            @Override
            public void visit(Column tableColumn) {
                super.visit(tableColumn);
                expression_item_stack.push(tableColumn.toString());
            }

            @Override
            public void visit(LongValue longValue) {
                super.visit(longValue);
                expression_item_stack.push(longValue.toString());
            }

            @Override
            public void visit(EqualsTo equalsTo) {
                super.visit(equalsTo);
                boolean_operator_stack.push("=");
            }

            @Override
            public void visit(NotEqualsTo notEqualsTo) {
                super.visit(notEqualsTo);
                boolean_operator_stack.push("!=");
            }

            @Override
            public void visit(GreaterThan greaterThan) {
                super.visit(greaterThan);
                boolean_operator_stack.push(">");
            }

            @Override
            public void visit(GreaterThanEquals greaterThanEquals) {
                super.visit(greaterThanEquals);
                boolean_operator_stack.push(">=");
            }

            @Override
            public void visit(MinorThan minorThan) {
                super.visit(minorThan);
                boolean_operator_stack.push("<");
            }

            @Override
            public void visit(MinorThanEquals minorThanEquals) {
                super.visit(minorThanEquals);
                boolean_operator_stack.push("<=");
            }


        };

        StringBuilder b = new StringBuilder();
        expressionDeParser.setBuffer(b);
        parseExpression.accept(expressionDeParser);


        return expression_stack;
    }


}
