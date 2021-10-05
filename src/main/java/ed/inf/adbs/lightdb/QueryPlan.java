package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * the whole query plan
 * Although some uncommon functions are not implemented, it essentially shows the logic for the most cases.
 * The specific logic is described in every judgement clause(if else).
 */
public class QueryPlan {
    public HashMap<String, Table> dataCatalog;
    public FromItem fromItem;
    public Expression expression;
    public List<SelectItem> selectItems;
    public List<Join> joins;
    public OrderByElement orderByElement;
    public Distinct distinct;
    public String outputFIle;
    public HashMap<String, String> aliases;

    /**
     * Receive every required parameters from interpreter.
     * Try to handle aliases but this function is not completely implemented.
     * Running the query with aliases may cause some issues but the most basic query with aliases may work as normal.
     * * @param dataCatalog data structure from the scheme
     * @param fromItem fromItem from the plainselect
     * @param expression WHERE expression
     * @param selectItems selectItems from the plainselect
     * @param joins joinsi from the plainselect
     * @param orderByElement orderByElement from the plainselect
     * @param distinct distinct from the plainselect
     * @param outputFIle output file path
     */
    public QueryPlan(HashMap<String, Table> dataCatalog, FromItem fromItem, Expression expression, List<SelectItem> selectItems, List<Join> joins, OrderByElement orderByElement, Distinct distinct, String outputFIle) {
        this.dataCatalog = dataCatalog;
        this.fromItem = fromItem;
        this.expression = expression;
        this.selectItems = selectItems;
        this.joins = joins;
        this.orderByElement = orderByElement;
        this.distinct = distinct;
        this.outputFIle = outputFIle;
        this.aliases = new HashMap<>();
        String[] split_fromItem = fromItem.toString().split(" ");
        //find aliases in fromitem
        if (split_fromItem.length == 2) {
            System.out.println(fromItem);
            aliases.put(split_fromItem[1], split_fromItem[0]);
        }
        //traverse joins to find aliases
        for (int i = 0; i < joins.size(); i++) {
            String[] split_join = joins.get(i).toString().split(" ");
            if (split_join.length == 2) {
                aliases.put(split_join[1], split_join[0]);
            }
        }
        System.out.println(aliases);
    }

    /**
     * Processing the components from the plainselect and choose which case and how to process
     * @throws IOException
     * @throws JSQLParserException
     */
    public void queryPlan() throws IOException, JSQLParserException {
        //judge whether WHERE
        //no WHERE means no SelectOperator
        if (expression == null) {
            //whether join
            if (joins.size() == 0) {
                //no join means only single table
                //whether project
                if (selectItems.size() == 1 && selectItems.get(0).toString().equals("*")) {
                    //SELECT *, no need ProjectOperator
                    //only scan the table in fromItem
                    String table_for_scan_name;
                    if (aliases == null) {
                        table_for_scan_name = fromItem.toString();
                    } else {
                        table_for_scan_name = fromItem.toString().split(" ")[0];
                    }
                    ScanOperator scan = new ScanOperator(dataCatalog.get(table_for_scan_name), outputFIle);
                    scan.dump();

                } else {
                    //single table ProjectOperator
                    String table_for_scan_name;
                    if (aliases == null) {
                        table_for_scan_name = fromItem.toString();
                    } else {
                        table_for_scan_name = fromItem.toString().split(" ")[0];
                    }
                    ScanOperator scan = new ScanOperator(dataCatalog.get(table_for_scan_name), outputFIle);
                    ProjectOperator proj = new ProjectOperator(scan, dataCatalog.get(table_for_scan_name), outputFIle, selectItems, expression, dataCatalog);
                    proj.dump();

                }
            } else {
                //no WHERE
                //multiple tables, have joins but no join condition, no select conditioin as well
                //crosss product join
                //whether SELECT *
                if (selectItems.size() == 1 && selectItems.get(0).toString().equals("*")) {
                    //SELECT *, join multiple scanOperator(cross product)
                    String table_for_leftscan_name;
                    String table_for_rightscan_name;
                    if (aliases == null) {
                        table_for_leftscan_name = fromItem.toString();
                        table_for_rightscan_name = joins.get(0).toString();
                    } else {
                        table_for_leftscan_name = fromItem.toString().split(" ")[0];
                        table_for_rightscan_name = joins.get(0).toString().split(" ")[0];
                    }
                    ScanOperator leftscan = new ScanOperator(dataCatalog.get(table_for_leftscan_name), outputFIle);
                    ScanOperator rightscan = new ScanOperator(dataCatalog.get(table_for_rightscan_name), outputFIle);
                    JoinOperator join1 = new JoinOperator(leftscan, rightscan, fromItem, joins, outputFIle, expression, dataCatalog, aliases);
                    if (joins.size() == 1) {
                        //two table join
                        join1.dump();
                    } else {
                        //multiple tables join
                        for (int i = 1; i < joins.size(); i++) {
                            //revursively invoking left joinOperator
                            String table_for_newright_name;
                            if (aliases == null){
                            table_for_newright_name = joins.get(i).toString();}
                            else{
                                table_for_newright_name=joins.get(i).toString().split(" ")[0];
                            }
                            ScanOperator newright = new ScanOperator(dataCatalog.get(table_for_newright_name), outputFIle);
                            join1 = new JoinOperator(join1, newright, fromItem, joins, outputFIle, expression, dataCatalog, aliases);
                            join1.dump();
                        }
                    }

                } else {
                    //SELECT have condition, need calling projectOperator, the remaining steps are the same as in if clause
                    String table_for_leftscan_name;
                    String table_for_rightscan_name;
                    if (aliases == null) {
                        table_for_leftscan_name = fromItem.toString();
                        table_for_rightscan_name = joins.get(0).toString();
                    } else {
                        table_for_leftscan_name = fromItem.toString().split(" ")[0];
                        table_for_rightscan_name = joins.get(0).toString().split(" ")[0];
                    }
                    ScanOperator leftscan = new ScanOperator(dataCatalog.get(table_for_leftscan_name), outputFIle);
                    ScanOperator rightscan = new ScanOperator(dataCatalog.get(table_for_rightscan_name), outputFIle);
                    JoinOperator join1 = new JoinOperator(leftscan, rightscan, fromItem, joins, outputFIle, expression, dataCatalog, aliases);
                    if (joins.size() == 1) {
                        //two tables join
                        ProjectOperator proj = new ProjectOperator(join1, dataCatalog.get(table_for_leftscan_name), outputFIle, selectItems, expression, dataCatalog);
                        proj.dump();
                    } else {
                        //multiple tables join
                        for (int i = 1; i < joins.size(); i++) {
                            //recursively calling the left join
                            String table_for_newright_name;
                            if (aliases == null){
                                table_for_newright_name = joins.get(i).toString();}
                            else{
                                table_for_newright_name=joins.get(i).toString().split(" ")[0];
                            }
                            ScanOperator newright = new ScanOperator(dataCatalog.get(table_for_newright_name), outputFIle);
                            join1 = new JoinOperator(join1, newright, fromItem, joins, outputFIle, expression, dataCatalog, aliases);
                        }
                        ProjectOperator proj = new ProjectOperator(join1, dataCatalog.get(table_for_leftscan_name), outputFIle, selectItems, expression, dataCatalog);
                        proj.dump();
                    }
                }


            }
        } else {
            //have WHERE
            //judge whether join
            if (joins.size() == 0) {
                //single table, so must have selectOperator, judge whether project
                if (selectItems.size() == 1 && selectItems.get(0).toString().equals("*")) {
                    //SELECT *, no need to call project
                    //only select the only table in fromitem
                    String table_for_scan_name;
                    if (aliases == null) {
                        table_for_scan_name = fromItem.toString();
                    } else {
                        table_for_scan_name = fromItem.toString().split(" ")[0];
                    }
                    SelectOperator select = new SelectOperator(dataCatalog.get(table_for_scan_name), outputFIle, expression, dataCatalog);
                    select.dump();
                } else {
                    //single table select and project
                    String table_for_scan_name;
                    if (aliases == null) {
                        table_for_scan_name = fromItem.toString();
                    } else {
                        table_for_scan_name = fromItem.toString().split(" ")[0];
                    }
                    SelectOperator scan = new SelectOperator(dataCatalog.get(table_for_scan_name), outputFIle, expression, dataCatalog);
                    ProjectOperator proj = new ProjectOperator(scan, dataCatalog.get(table_for_scan_name), outputFIle, selectItems, expression, dataCatalog);
                    proj.dump();
                }
            } else {

                //multiple tables, so have join, may have join condition, may have select conditoin

                //if(only join condition), the length of select condition is 0

                //if(only select condition), the length of join condition is 0

                //if(both join and select), the lengths of both join and select are not 0



            }

        }
    }

    /**
     * incomplete method to replace the aliases with their original name
     */
    public void haveATry(){
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
}
