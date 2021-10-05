package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Read the SQL query from the input file and parse it
 */
public class Interpreter {
    /**
     * Read and parse the query and get components from the plainselect
     * @throws IOException
     * @throws JSQLParserException
     */
    public String databaseDir;
    public String inputFile;
    public String outoutFile;
    public Interpreter(String databaseDir,String inputFile,String outoutFile) throws IOException, JSQLParserException {
        this.databaseDir=databaseDir;
        this.inputFile=inputFile;
        this.outoutFile=outoutFile;
//
//        String databaseDir = "samples/db";
//        String inputFile = "samples/input/query10.sql";
//        String outputFIle = "samples/output/query10.csv";
        HashMap<String, Table> dataCatalog = readDbcatalog(databaseDir);
        PlainSelect plainSelect = parseQuery(inputFile);
        FromItem fromItem = plainSelect.getFromItem();
        Expression expression = plainSelect.getWhere();
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        List<Join> joins = plainSelect.getJoins();
        OrderByElement orderByElement = (OrderByElement) plainSelect.getOrderByElements();
        Distinct distinct = plainSelect.getDistinct();
        System.out.println(plainSelect);
        System.out.println(fromItem);
        System.out.println(expression);
        System.out.println(selectItems);
        System.out.println(joins);
        System.out.println(distinct);
        System.out.println(expression.toString().split(" AND "));
//        QueryPlan queryPlan = new QueryPlan(dataCatalog, fromItem, expression, selectItems, joins, orderByElement, distinct,outputFIle);
//        queryPlan.queryPlan();
    }

    /**
     * Read the query form input file and parse it to plainselect
     * @param inputFile inputFile path
     * @return
     */
    public static PlainSelect parseQuery(String inputFile) {
        Statement statement = null;
        try {
            statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
        } catch (JSQLParserException | FileNotFoundException e) {
            System.err.println("Exception occurred during parsing");
            e.printStackTrace();
        }
        Select select = (Select) statement;
        return (PlainSelect) select.getSelectBody();
    }

    /**
     * Read the data structure(calling readFile method) from the scheme.txt file and store then in a hashmap<table name, Table object>
     * @param databaseDir databaseDir path
     * @return
     */
    public static HashMap<String, Table> readDbcatalog(String databaseDir) {
        HashMap<String, Table> dbcatalog = new HashMap<>();
        //read every line of the schema file and store them in an array
        String[] tableList = readFile(databaseDir + "/schema.txt").toArray(new String[0]);

        for (int i = 0; i < tableList.length; i++) {
            //split every line of the schema file
            //put the first element(table name) as key
            String[] split_tablelist = tableList[i].split(" ");
            String[] attributesName = new String[split_tablelist.length - 1];
            //the remaining elemtns as an attributes array
            for (int j = 0; j < attributesName.length; j++) {
                attributesName[j] = split_tablelist[j + 1];
            }
            //declare a new Table object and assign table name, attributes array, file path to the object
            Table table = new Table(split_tablelist[0], attributesName, databaseDir + "/data/" + split_tablelist[0] + ".csv");
            dbcatalog.put(split_tablelist[0], table);
        }

        return dbcatalog;
    }

    /**
     * Read the scheme and return a string stream to the above method
     * @param path the file path
     * @return
     */
    public static ArrayList<String> readFile(String path) {
        ArrayList<String> string_stream = new ArrayList<>();
        try (FileReader reader = new FileReader(path);
             BufferedReader br = new BufferedReader(reader)
        ) {
            String line;

            while ((line = br.readLine()) != null) {
                string_stream.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return string_stream;
    }

}
