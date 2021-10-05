package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * project the tuples from the child operator
 */
public class ProjectOperator extends Operator {
    public List<SelectItem> selectItems;
    public ArrayList<String> columns = new ArrayList<String>();
    public Expression expression;
    public Table table_for_scan;
    public String outputFIle;
    public HashMap<String, Table> dataCatalog;
    public ScanOperator scanOperator;
    public SelectOperator selectOperator;
    public String outputFile;
    public Operator op;
    public FromItem fromItem;
    public List<Join> joins;
    public boolean single_multi;
    ArrayList<String> combine_tuple_attributes;

    /**
     * receive required parameters
     * The first constructor only for receiving scan or select operator
     * columns is a string arraylist storing all the selectItems.It can be considered as the list version of selectItems.
     * @param op the child operator
     * @param table_for_scan the single table for scan
     * @param outputFile output file path
     * @param selectItems selectItems from the query
     * @param expression WHERE expression
     * @param dataCatalog data structure from the scheme
     * @throws IOException
     */
    public ProjectOperator(Operator op, Table table_for_scan, String outputFile, List<SelectItem> selectItems, Expression expression, HashMap<String, Table> dataCatalog) throws IOException {
        this.selectItems = selectItems;
        this.expression = expression;
        this.table_for_scan = table_for_scan;
        this.outputFIle = outputFile;
        this.dataCatalog = dataCatalog;
        this.op = op;
        this.outputFile = outputFile;
        this.single_multi = true;
        if (selectItems.toString() == "*") {
            columns.add("*");
        } else {
            for (int i = 0; i < selectItems.size(); i++) {
                columns.add(selectItems.get(i).toString());
            }
        }
        System.out.println(columns);
    }

    /**
     * The second construct only for join child operator,
     * since the child will return a long tuple that cannot find the index in the original dataCatalog
     * using the original method may cause IndexOutofBounds error
     * This construct will build a long list to store all the attribues index
     * For example, if Sailors join Boats(A,B,C,D,E,F)
     * the maximum index for Sailors is 2 but the actual index when we find the attribues in Sailors may over 2
     * So we should build a new dataCatalog to map the new index and the new attributes
     * like(1,2,3,4,5,6) and (A,B,C,D,E,F)
     * The idea is the same in other classes involving process very long joined tuples
     * @param join child join operator
     * @param table_for_scan the single table object for scan
     * @param outputFile output file path
     * @param selectItems selectItems from the plainselect
     * @param expression WHERE expression
     * @param dataCatalog dataCatalog from scheme
     * @param fromItem fromItem from the plainselect
     * @param joins joins from the plainselect
     * @throws IOException
     */
    public ProjectOperator(JoinOperator join, Table table_for_scan, String outputFile, List<SelectItem> selectItems, Expression expression, HashMap<String, Table> dataCatalog, FromItem fromItem, List<Join> joins) throws IOException {
        this.selectItems = selectItems;
        this.expression = expression;
        this.table_for_scan = table_for_scan;
        this.outputFIle = outputFile;
        this.dataCatalog = dataCatalog;
        this.op = join;
        this.outputFile = outputFile;
        this.fromItem = fromItem;
        this.joins = joins;
        this.single_multi = false;
        if (selectItems.toString() == "*") {
            columns.add("*");
        } else {
            for (int i = 0; i < selectItems.size(); i++) {
                columns.add(selectItems.get(i).toString());
            }
        }
//        System.out.println(columns);
        String[] fromitem_attributes = dataCatalog.get(fromItem.toString()).attributes;
        combine_tuple_attributes = new ArrayList<>(Arrays.asList(fromitem_attributes));
        for (int i = 0; i < joins.size(); i++) {
            combine_tuple_attributes.addAll(Arrays.asList(dataCatalog.get(joins.get(i).toString()).attributes));
        }

    }

    /**
     * Repeatedly calling the next tuple and store all the tuples in an arralylist
     * write the result to file
     * @throws IOException
     * @throws JSQLParserException
     */
    @Override
    public void dump() throws IOException, JSQLParserException {
        ArrayList<Tuple> tuplelist = new ArrayList<>();
        while (true) {
            Tuple tuple = getNextTuple();
            if (tuple == null) {
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
     * Receive the tuple from the child operator and judge whether it meets the project condition
     * There will be an error in this method if two tables have the same column name
     * like Sailors.B, Boats.B
     * @return Returen the qualified tuple to the dump method
     * @throws IOException
     * @throws JSQLParserException
     */
    @Override
    public Tuple getNextTuple() throws IOException, JSQLParserException {
//        if (expression == null) {
        if (columns.get(0) == "*") {
            Tuple check_null = op.getNextTuple();
            if (check_null != null) {
                return check_null;
            } else {
                return null;
            }
        } else {
            Tuple tuple = op.getNextTuple();
            if (tuple == null) {
                return null;
            } else {
                int[] originalTupleAttributes = tuple.attributes;
                ArrayList<Integer> newTuple = new ArrayList<Integer>();

                for (int i = 0; i < columns.size(); i++) {
                    String[] table_column = columns.get(i).split("\\.");
                    if (single_multi) {
                        String[] tableAttributes = dataCatalog.get(table_column[0]).attributes;
                        for (int j = 0; j < tableAttributes.length; j++) {
                            if (tableAttributes[j].equals(table_column[1])) {
                                newTuple.add(originalTupleAttributes[j]);
                            }
                        }
                    } else {
                        for (int j = 0; j < combine_tuple_attributes.size(); j++) {
                            //there will be an error here if two tables have the same column name
                            //like Sailors.B, Boats.B
                            if (combine_tuple_attributes.get(j).equals(table_column[1])) {
                                newTuple.add(originalTupleAttributes[j]);
                            }
                        }
                    }
                }
                if (newTuple.size() != 0) {
                    System.out.println(newTuple);
                    int[] intTuple = new int[newTuple.size()];
                    for (int index = 0; index < newTuple.size(); index++) {
                        intTuple[index] = newTuple.get(index);
                    }
                    Tuple filtered_tuple = new Tuple(intTuple);
//                System.out.println(filtered_tuple.attributes[1]);
                    return filtered_tuple;
                } else {
                    return null;
                }
            }
        }
    }

    /**
     * Reset the child operator
     * @throws IOException
     * @throws JSQLParserException
     */
    @Override
    public void reset() throws IOException, JSQLParserException {
        op.reset();
    }
}
