package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Eliminate the duplicate tuples according to DISTINCT
 */
public class DuplicateEliminationOperator extends Operator {
    public ArrayList<Tuple> tuplelist;
    public ArrayList<String> combine_tuple_attributes;
    List<SelectItem> selectItems;

    /**
     * Receive the tuple list from the SortOperator
     * Elminate the duplicate tuple according to the columns in selectItems
     * For each seletItem, find the index in the long tuple attributes(e.g. A,B,C,D,E,F)
     * Two for nested loops to traverse the tuple list to find whether the integers on that index are the same in both tuples
     * @param sort sort operator
     * @param selectItems selectItems from the query
     */
    public DuplicateEliminationOperator(SortOperator sort, List<SelectItem> selectItems) {
        this.tuplelist = sort.tuplelist;
        this.combine_tuple_attributes = sort.combine_tuple_attributes;
        this.selectItems = selectItems;
        for (int i = 0; i < selectItems.size(); i++) {
            String[] split_selectitem = selectItems.get(i).toString().split("\\.");
            for (int combine_index = 0; combine_index < combine_tuple_attributes.size(); combine_index++) {
                int compareindex;
                if (split_selectitem[1] == combine_tuple_attributes.get(combine_index)) {
                    compareindex = combine_index;
                    int original_sizeof_tuplelist = tuplelist.size();
                    for (int former = 0; former < original_sizeof_tuplelist; former++) {
                        for (int latter = former + 1; latter < original_sizeof_tuplelist; latter++) {
                            if (tuplelist.get(former).attributes[compareindex] == tuplelist.get(latter).attributes[compareindex]) {
                                tuplelist.remove(latter);
                                original_sizeof_tuplelist = original_sizeof_tuplelist - 1;
                            }
                        }
                    }
                }
            }

        }

    }

    @Override
    public void dump() throws IOException, JSQLParserException {

    }

    @Override
    public Tuple getNextTuple() throws IOException, JSQLParserException {
        return null;
    }

    @Override
    public void reset() throws IOException, JSQLParserException {

    }
}
