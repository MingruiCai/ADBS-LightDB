package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Receive left and right abstract operator(scan or select)
 * Join the left tuple and right tuple together
 */
public class JoinOperator extends Operator {
    public Expression expression;
    public Operator leftOperator;
    public Operator rightOperator;
    public Table leftTable;
    public Table rightTable;
    public String outputFile;
    public HashMap<String, Table> dataCatalog;
    public Tuple leftTuple;
    public HashMap<String, String> aliases;

    /**
     * Receive all the required parameters, and put them to some global variables
     * @param leftOperator left child
     * @param rightOperator right child
     * @param fromItem fromItem in the query
     * @param joins joins in the query
     * @param outputFile output file path
     * @param expression WHERE expression in the query
     * @param dataCatalog data structure the read from the scheme
     * @param aliases map to find aliases(e.g. <S1, Sailors>)
     * @throws IOException
     * @throws JSQLParserException
     */
    public JoinOperator(Operator leftOperator, Operator rightOperator, FromItem fromItem, List<Join> joins, String outputFile, Expression expression, HashMap<String, Table> dataCatalog,HashMap<String, String> aliases) throws IOException, JSQLParserException {
        this.outputFile = outputFile;
        this.aliases=aliases;
        if(aliases==null){
            this.leftTable = dataCatalog.get(fromItem.toString());
            this.rightTable = dataCatalog.get(joins.get(0).toString());
        }else{
            this.leftTable=dataCatalog.get(fromItem.toString().split(" ")[0]);
            this.rightTable=dataCatalog.get(joins.get(0).toString().split(" ")[0]);
        }
        this.leftOperator=leftOperator;
        this.rightOperator=rightOperator;
        leftTuple = leftOperator.getNextTuple();
        this.expression = expression;
        this.dataCatalog = dataCatalog;
    }

    /**
     * Repeatedly calling getnexttuple method and store all the tuple together
     * Write the result to a csv file in output path
     * @throws IOException
     * @throws JSQLParserException
     */
    @Override
    public void dump() throws IOException, JSQLParserException {
        ArrayList<Tuple> tuplelist = new ArrayList<>();
        while (true) {
            Tuple tuple = getNextTuple();
            if (tuple==null) {
                break;
            } else {
                tuplelist.add(tuple);
            }
        }
        File f = new File("samples/output");
        if (!f.isDirectory()) f.mkdirs();
        PrintStream p = new PrintStream(new FileOutputStream(new File(outputFile)));
        for (int i = 0; i < tuplelist.size(); i++) {
            String readytowrite_tuple = "";
            for (int j = 0; j < tuplelist.get(i).attributes.length; j++) {
                readytowrite_tuple = readytowrite_tuple + tuplelist.get(i).attributes[j] + ",";
            }
            readytowrite_tuple.substring(0, readytowrite_tuple.length() - 1);
            p.println(readytowrite_tuple);
        }
        p.close();
    }

    /**
     * get the next tuple
     * @return return the next tuple after the join(a long joined tuple) if it meets the join condition
     * @throws IOException
     * @throws JSQLParserException
     */
    @Override
    public Tuple getNextTuple() throws IOException, JSQLParserException {
        Tuple rightTuple = rightOperator.getNextTuple();

        if (leftTuple == null) {
            return null;
        }
        if (rightTuple == null) {
            rightOperator.reset();
            this.leftTuple = leftOperator.getNextTuple();
            if (leftTuple == null) {
                return null;
            } else {
                return getNextTuple();
            }
        } else {
            JoinVisitor joinvisitor = new JoinVisitor(leftTuple, rightTuple, leftTable, rightTable, expression, dataCatalog);
            if (joinvisitor.evaluate(expression.toString())) {
                int[] newtuple_attributes = new int[leftTuple.attributes.length + rightTuple.attributes.length];
                for (int i = 0; i < leftTuple.attributes.length; i++) {
                    newtuple_attributes[i] = leftTuple.attributes[i];
                }
                for (int j = leftTuple.attributes.length; j < leftTuple.attributes.length + rightTuple.attributes.length; j++) {
                    newtuple_attributes[j] = rightTuple.attributes[j - leftTuple.attributes.length];
                }
                return new Tuple(newtuple_attributes);
            } else {
                return this.getNextTuple();
            }
        }

    }

    /**
     * reset both child operators
     * and call the getNextTuple method for the left child to start the traverse loop.
     * @throws IOException
     * @throws JSQLParserException
     */
    @Override
    public void reset() throws IOException, JSQLParserException {
            this.leftOperator.reset();
            this.rightOperator.reset();
            leftOperator.getNextTuple();
    }


}
