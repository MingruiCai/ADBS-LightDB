package ed.inf.adbs.lightdb;

/**
 * Table object contains its name, attribute and its file path
 */
public class Table {
    public String name;
    public String[] attributes;
    public String filepath;

    public Table(String name, String[] attributes, String filepath) {
        this.name = name;
        this.attributes = attributes;
        this.filepath = filepath;
    }
}
