package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * select the tuple according to the where expression from scan operator
 */
public class SelectOperator extends Operator {
    public Table table_for_scan;
    public String outputFile;
    public FileReader reader;
    public BufferedReader bufferedReader;
    public ScanOperator scanOperator;
    public net.sf.jsqlparser.expression.Expression expression;
    public HashMap<String, Table> dataCatalog;
    public Boolean end;

    /**
     * receive parameters
     * @param table_for_scan table object for scan
     * @param outputFile output file path
     * @param expression WHERE expression
     * @param dataCatalog data structure from the scheme
     * @throws IOException
     */
    public SelectOperator(Table table_for_scan, String outputFile, Expression expression, HashMap<String, Table> dataCatalog) throws IOException {
        this.table_for_scan = table_for_scan;
        this.outputFile = outputFile;
        this.reader = new FileReader(table_for_scan.filepath);
        this.bufferedReader = new BufferedReader(reader);
        this.scanOperator = new ScanOperator(table_for_scan, outputFile);
        this.expression = expression;
        this.dataCatalog = dataCatalog;
        end = false;
    }

    /**
     * Repeatedly call the getNextTuple method and store all the tuples in an arraylist
     * write to file
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
     * check whether the tuple from scanOperator meet the select condition
     * @return Return the tuple that meet the select condition
     * @throws IOException
     * @throws JSQLParserException
     */
    @Override
    public Tuple getNextTuple() throws IOException, JSQLParserException {
        Tuple nextTuple = scanOperator.getNextTuple();
        if (nextTuple != null) {
            Visitor visitor = new Visitor(nextTuple, expression, dataCatalog);
            if (visitor.evaluate(expression.toString())) {
//            System.out.println("fk");
                return nextTuple;
            } else {
                return this.getNextTuple();
//            System.out.println("exclude this tuple");
            }
        }
        return null;
    }

    /**
     * reset selectOperator by new a child operator
     * @throws IOException
     */
    @Override
    public void reset() throws IOException {
        this.scanOperator=new ScanOperator(table_for_scan,outputFile);
    }
}
