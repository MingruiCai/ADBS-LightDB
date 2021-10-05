package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.JSQLParserException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Abstract class of Operator
 * It requires its child operator to implement all of the abstract methods
 */
abstract class Operator {
    public abstract void dump() throws IOException, JSQLParserException;

    public abstract Tuple getNextTuple() throws IOException, JSQLParserException;

    public abstract void reset() throws IOException, JSQLParserException;

}
