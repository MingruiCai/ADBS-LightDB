package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.statement.select.FromItem;

import java.io.*;
import java.util.ArrayList;

/**
 * scan a single table
 */
public class ScanOperator extends Operator {
    public Table table_for_scan;
    public String outputFile;
    public FileReader reader;
    public BufferedReader br;

    /**
     * receive parameters
     * @param table_for_scan table object for scan
     * @param outputFile output file path
     * @throws IOException
     */
    public ScanOperator(Table table_for_scan, String outputFile) throws IOException {
        this.table_for_scan = table_for_scan;
        this.outputFile = outputFile;
        this.reader = new FileReader(table_for_scan.filepath);
        this.br = new BufferedReader(reader);
    }

    /**
     * Get all the tuples from getNextTuple method,
     * store them in a list and write the result to the file
     * @throws IOException
     */
    @Override
    public void dump() throws IOException {
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
        if (!f.isDirectory()) {
            f.mkdirs();
        }
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
     * Scan every tuple from a given table
     * @return return the next tuple from the table
     * @throws IOException
     */
    @Override
    public Tuple getNextTuple() throws IOException {
        String tuple = br.readLine();
        if (tuple != null) {
            String[] tupleString = tuple.split(",");
            int[] tupleint = new int[tupleString.length];
            for (int i = 0; i < tupleString.length; i++) {
                tupleint[i] = Integer.parseInt(tupleString[i]);
            }
            return new Tuple(tupleint);
        } else {
//            System.out.println("No more tuples");
            return null;
        }
    }

    /**
     * reset the scan operator by renew fileReader and bufferReader
     * @throws IOException
     */
    @Override
    public void reset() throws IOException {
        this.reader = new FileReader(table_for_scan.filepath);
        this.br = new BufferedReader(reader);
    }

}
