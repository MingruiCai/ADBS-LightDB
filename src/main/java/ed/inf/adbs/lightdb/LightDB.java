package ed.inf.adbs.lightdb;
import net.sf.jsqlparser.JSQLParserException;
import java.io.IOException;



/**
 * Lightweight in-memory database system
 * only for calling interperter to start the processing
 */
public class LightDB {

    public static void main(String[] args) throws IOException, JSQLParserException {
        if (args.length != 3) {
            System.err.println("Usage: LightDB database_dir input_file output_file");
            return;
        }

        String databaseDir = args[0];
        String inputFile = args[1];
        String outputFile = args[2];
        //initialization
        Interpreter interpreter=new Interpreter(databaseDir,inputFile,outputFile);

    }
}
