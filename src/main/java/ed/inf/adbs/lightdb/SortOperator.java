package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;

/**
 * sort the final tuple list according to the orderbyElement
 */
public class SortOperator extends Operator {
    public Operator op;
    public String outputFile;
    public FromItem fromItem;
    public List<Join> joins;
    public HashMap<String, Table> dataCatalog;
    public ArrayList<String> combine_tuple_attributes;
    public OrderByElement orderByElement;
    public int orderby_index;
    ArrayList<Tuple> tuplelist;

    /**
     * Receive all the parameters and build a new index to map the long tuple
     * (detailed explaination and examples can be seen in the second constructor of project Operator )
     * @param op child opeartor
     * @param outputFile output file path
     * @param fromItem fromItem from plainselect
     * @param dataCatalog data structure from the scheme
     * @param joins joins from the plainselect
     * @param orderByElement orderbyelement from the plainselect
     */
    public SortOperator(Operator op, String outputFile, FromItem fromItem, HashMap<String, Table> dataCatalog, List<Join> joins, OrderByElement orderByElement) {
        this.op = op;
        this.outputFile = outputFile;
        this.fromItem = fromItem;
        this.dataCatalog = dataCatalog;
        this.joins = joins;
        this.orderByElement = orderByElement;
        String[] fromitem_attributes = dataCatalog.get(fromItem.toString()).attributes;
        combine_tuple_attributes = new ArrayList<>(Arrays.asList(fromitem_attributes));
        for (int i = 0; i < joins.size(); i++) {
            combine_tuple_attributes.addAll(Arrays.asList(dataCatalog.get(joins.get(i).toString()).attributes));
        }
    }

    /**
     * get all the tuples and sort the tuple list
     * the idea here is to find the orderbyelment in the long tuple and get its index
     * sort the whole arraylist by this the column correspoding to this index
     * @throws IOException
     * @throws JSQLParserException
     */
    @Override
    public void dump() throws IOException, JSQLParserException {
        this.tuplelist = new ArrayList<>();
        while (true) {
            Tuple tuple = getNextTuple();
            if (tuple == null) {
                break;
            } else {
                this.tuplelist.add(tuple);
            }
        }
        String[] split_orderby = orderByElement.toString().split("\\.");
        for (int i = 0; i < combine_tuple_attributes.size(); i++) {
            if (combine_tuple_attributes.get(i) == split_orderby[1]) {
                this.orderby_index = i;
            }
        }
        Collections.sort(this.tuplelist, new Comparator<Tuple>() {
            @Override
            public int compare(Tuple o1, Tuple o2) {
                return o2.attributes[orderby_index] - (o1.attributes[orderby_index]);
            }
        });
        File f = new File("samples/output");
        if (!f.isDirectory()) f.mkdirs();
        PrintStream p = new PrintStream(new FileOutputStream(new File(outputFile)));
        for (int i = 0; i < this.tuplelist.size(); i++) {
            String readytowrite_tuple = "";
            for (int j = 0; j < this.tuplelist.get(i).attributes.length; j++) {
                readytowrite_tuple = readytowrite_tuple + this.tuplelist.get(i).attributes[j] + ",";
            }
            readytowrite_tuple.substring(0, readytowrite_tuple.length() - 1);
            p.println(readytowrite_tuple);
        }
        p.close();
    }

    /**
     * get next tuple from child operator
     * @return tuple from child operator
     * @throws IOException
     * @throws JSQLParserException
     */
    @Override
    public Tuple getNextTuple() throws IOException, JSQLParserException {
        return op.getNextTuple();
    }

    /**
     * reset sort operator by resetting child operator
     * @throws IOException
     * @throws JSQLParserException
     */
    @Override
    public void reset() throws IOException, JSQLParserException {
        op.reset();
    }
}
