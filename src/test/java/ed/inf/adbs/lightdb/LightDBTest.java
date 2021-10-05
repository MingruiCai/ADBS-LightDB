package ed.inf.adbs.lightdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.HashMap;

/**
 * Unit test for simple LightDB.
 */
public class LightDBTest {

    /**
     * Rigorous Test :-)
     */
    String databaseDir = "samples/db";
    String inputFile = "samples/input/query10.sql";

    String outputFIle = "samples/output/query10.csv";
    HashMap<String, Table> dataCatalog = readDbcatalog(databaseDir);
    PlainSelect plainSelect = parseQuery(inputFile);
    FromItem fromItem = plainSelect.getFromItem();
    Expression expression = plainSelect.getWhere();
    List<SelectItem> selectItems = plainSelect.getSelectItems();
    List<Join> joins = plainSelect.getJoins();
    Distinct distinct = plainSelect.getDistinct();

    @Test
    public void ScanTest() throws JSQLParserException, IOException {
        ScanOperator scanop = new ScanOperator(dataCatalog.get(fromItem.toString()), outputFIle);
//        scanop.dump();
//        assertEquals(101, scanop.getNextTuple().attributes[0]);
        assertEquals(102, scanop.getNextTuple().attributes[0]);
        scanop.reset();
        assertEquals(101, scanop.getNextTuple().attributes[0]);
    }

    @Test
    public void VisitorTest() throws IOException, JSQLParserException {
        ScanOperator scanOperator = new ScanOperator(dataCatalog.get(fromItem.toString()), outputFIle);
        Visitor visitor = new Visitor(scanOperator.getNextTuple(), expression, dataCatalog);
        assertEquals(false, visitor.evaluate("4=4 AND 4>5"));
        assertEquals(true, visitor.evaluate("Boats.E = 2"));
        Visitor visitor2 = new Visitor(scanOperator.getNextTuple(), expression, dataCatalog);
        assertEquals(false, visitor2.evaluate("Boats.E = 2"));
    }

    @Test
    public void SelectTest() throws IOException, JSQLParserException {
        SelectOperator selectOperator = new SelectOperator(dataCatalog.get(fromItem.toString()), outputFIle, expression, dataCatalog);
        System.out.println(expression);
        assertEquals(200, selectOperator.getNextTuple().attributes[1]);
        assertEquals(107, selectOperator.getNextTuple().attributes[0]);
        selectOperator.reset();
        assertEquals(101, selectOperator.getNextTuple().attributes[0]);

    }

    @Test
    public void ProjectTest() throws IOException, JSQLParserException {
//        ScanOperator scan=new ScanOperator(dataCatalog.get(fromItem.toString()),outputFIle);
        ScanOperator leftscan = new ScanOperator(dataCatalog.get(fromItem.toString()), outputFIle);
        ScanOperator rightscan = new ScanOperator(dataCatalog.get(joins.get(0).toString()), outputFIle);
//        JoinOperator joinOperator = new JoinOperator(leftscan, rightscan, fromItem, joins, outputFIle, expression, dataCatalog);
//        ProjectOperator projectOperator = new ProjectOperator(joinOperator, dataCatalog.get(fromItem.toString()), outputFIle, selectItems, expression, dataCatalog, fromItem, joins);

    }

    @Test
    public void JoinTest() throws IOException, JSQLParserException {
        System.out.println("fromItem = " + fromItem);
        System.out.println("joins = " + joins);
        System.out.println("where = " + expression);
        ScanOperator leftscan = new ScanOperator(dataCatalog.get(fromItem.toString()), outputFIle);
        ScanOperator rightscan = new ScanOperator(dataCatalog.get(joins.get(0).toString()), outputFIle);
//        JoinOperator joinOperator = new JoinOperator(leftscan, rightscan, fromItem, joins, outputFIle, expression, dataCatalog);
//        System.out.println(joinOperator.getNextTuple().attributes[0]);

//        joinOperator.getNextTuple();
//        joinOperator.getNextTuple();
//        assertEquals(101, joinOperator.getNextTuple().attributes[3]);
//        assertEquals(107, joinOperator.getNextTuple().attributes[3]);

    }

    @Test
    public void interpreterTest() throws IOException, JSQLParserException {
//        Interpreter interpreter = new Interpreter();
        HashMap<String, String> aliases = new HashMap<>();
        String[] split_fromItem = fromItem.toString().split(" ");
        //find any aliases in fromItem
        if (split_fromItem.length == 2) {
            System.out.println(fromItem);
            aliases.put(split_fromItem[1], split_fromItem[0]);
        }
        //traverse the joins to find any aliases
        for (int i = 0; i < joins.size(); i++) {
            String[] split_join = joins.get(i).toString().split(" ");
            if (split_join.length == 2) {
                aliases.put(split_join[1], split_join[0]);
            }
        }
        System.out.println(aliases);
        //replace aliases in the query
        String newQuery = "SELECT ";
        //judge is there DISTINCT behind SELECT
        if (distinct != null) newQuery = newQuery + "DISTINCT ";
        //judge wether *
        if (selectItems.size() == 1 && selectItems.get(0).toString().equals("*")) {
            newQuery = newQuery + "* ";
        } else {
            //replace aliases in SELECT
            for (int i = 0; i < selectItems.size(); i++) {
                String[] split_selectItems = selectItems.get(i).toString().split(" ");
                if (split_selectItems.length == 2) {
                    // Iterating entries using a For Each loop
                    for (String key : aliases.keySet()) {
                        System.out.println("Key = " + key);
                        if (key.equals(split_selectItems[1])) {
                            newQuery = newQuery + aliases.get(key) + ",";
                        }
                    }
                }
            }
            newQuery.substring(0, newQuery.length() - 1);
        }
        //replace the aliases in fromItem
        newQuery = newQuery + " FROM ";
        if (split_fromItem.length == 2) {
            for (String key : aliases.keySet()) {
//                System.out.println("Key = " + key);
                if (key.equals(split_fromItem[1])) {
                    newQuery = newQuery + aliases.get(key);
                }
            }
        }
        //replace the aliases in joins
        if (joins != null) {
            for (int i = 0; i < joins.size(); i++) {
                String[] split_joins = joins.get(i).toString().split(" ");
                if (split_joins.length == 2) {
                    // Iterating entries using a For Each loop
                    for (String key : aliases.keySet()) {
                        System.out.println("Key = " + key);
                        if (key.equals(split_joins[1])) {
                            newQuery = newQuery + " ," + aliases.get(key);
                        }
                    }
                }
            }
        }

        //replace aliases in WHERE
        if(expression!=null){
            if(expression.toString().contains("AND")){
                //WHERE includs AND
            }else{
                //only single operation in WHERE
                String[] split_where=expression.toString().split(" ");
                String left_column=split_where[0].toString();
                String right_column=split_where[2].toString();
                String[] left_column_split=left_column.split("\\.");
                String[] right_column_split=right_column.split("\\.");

            }
        }

    }

    @Test
    public void only_run_Interpreter() throws IOException, JSQLParserException {
//        Interpreter interpreter = new Interpreter();

    }

    @Test
    public void splitvisTest() throws JSQLParserException {
        SplitWhereVisitor sp = new SplitWhereVisitor();
        Stack whereStacksp = sp.SplitWhere_evaluate(expression.toString());
        int stack_size = whereStacksp.size();
        for (int i = 0; i < stack_size; i++) {
            System.out.println(whereStacksp.pop());
        }
    }

    @Test
    public void haveAtry() {
        String temp = "Sailors  ";
        String[] split_join = temp.split(" ");
        System.out.println(split_join.length);
        PlainSelect pl = new PlainSelect();
    }

    public static PlainSelect parseQuery(String inputFile) {
        Statement statement = null;
        try {
            statement = CCJSqlParserUtil.parse(new FileReader(inputFile));

        } catch (JSQLParserException | FileNotFoundException e) {
            System.err.println("Exception occurred during parsing");
            e.printStackTrace();
        }
        Select select = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        return plainSelect;
    }

    public static PlainSelect parseAliasesQuery(String noaliases_query) {
        Statement statement = null;
        try {
            statement = CCJSqlParserUtil.parse(noaliases_query);

        } catch (JSQLParserException e) {
            System.err.println("Exception occurred during parsing");
            e.printStackTrace();
        }
        Select select = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        return plainSelect;
    }

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

    public static ArrayList<String> readFile(String path) {
        //防止文件建立或读取失败，用catch捕捉错误并打印，也可以throw;
        //不关闭文件会导致资源的泄露，读写文件都同理
        //Java7的try-with-resources可以优雅关闭文件，异常时自动关闭文件；详细解读https://stackoverflow.com/a/12665271
        ArrayList<String> string_stream = new ArrayList<>();
        try (FileReader reader = new FileReader(path);
             BufferedReader br = new BufferedReader(reader) // 建立一个对象，它把文件内容转成计算机能读懂的语言
        ) {
            String line;
            //网友推荐更加简洁的写法
            while ((line = br.readLine()) != null) {
                // 一次读入一行数据
//                System.out.println(line);
                string_stream.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return string_stream;
    }
}
