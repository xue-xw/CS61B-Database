package db;

import javax.xml.crypto.Data;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Created by White Noise on 2017/3/2.
 */
public class Parse {
    private static final String REST = "\\s*(.*)\\s*",
            COMMA = "\\s*,\\s*",
            AND = "\\s+and\\s+";

    private static final Pattern CREATE_CMD = Pattern.compile("create table " + REST),
            LOAD_CMD = Pattern.compile("load " + REST),
            STORE_CMD = Pattern.compile("store " + REST),
            DROP_CMD = Pattern.compile("drop table " + REST),
            INSERT_CMD = Pattern.compile("insert into " + REST),
            PRINT_CMD = Pattern.compile("print " + REST),
            SELECT_CMD = Pattern.compile("select " + REST);

    private static final Pattern CREATE_NEW = Pattern.compile("(\\S+)\\s+\\(\\s*(\\S+\\s+\\S+\\s*" +
            "(?:,\\s*\\S+\\s+\\S+\\s*)*)\\)"),
            SELECT_CLS = Pattern.compile("([^,]+?(?:,[^,]+?)*)\\s+from\\s+" +
                    "(\\S+\\s*(?:,\\s*\\S+\\s*)*)(?:\\s+where\\s+" +
                    "([\\w\\s+\\-*/'<>=!.]+?(?:\\s+and\\s+" +
                    "[\\w\\s+\\-*/'<>=!.]+?)*))?"),
            CREATE_SEL = Pattern.compile("(\\S+)\\s+as select\\s+" +
                    SELECT_CLS.pattern()),
            INSERT_CLS = Pattern.compile("(\\S+)\\s+values\\s+(.+?" +
                    "\\s*(?:,\\s*.+?\\s*)*)");

    public static String main(String[] args) throws IOException {
        if (args.length != 1) {
            return ("Expected a single query argument");
        }
        return read(args[0]);
    }

    private static String read(String query) throws IOException {
        Matcher m;
        if ((m = LOAD_CMD.matcher(query)).matches()) {
            return loadTable(m.group(1));
        } else if ((m = PRINT_CMD.matcher(query)).matches()) {
            return printTable(m.group(1));
        } else if ((m = SELECT_CMD.matcher(query)).matches()) {
            return select(m.group(1));
        } else {
            return ("Malformed query: %s\n" + query);
        }
    }

    static String loadTable(String name) throws IOException {
        try {
            BufferedReader tableFile = new BufferedReader(new FileReader(name + ".tbl"));
        } catch (FileNotFoundException e) {
            return (name + ".tbl is not found");
        }
        BufferedReader tableFile = new BufferedReader(new FileReader(name + ".tbl"));
        String[] colNames = tableFile.readLine().split(",");
        HashMap<Integer, String[]> tCompile = new HashMap<>();
        int index = 0;
        while (tableFile.ready()) {
            String[] row = tableFile.readLine().split(",");
            index += 1;
            tCompile.put(index, row);
        }
        int numCol = colNames.length;
        Table ret = new Table(numCol, colNames);
        for (int i = 1; i <= index; i++) {
            ret.addRow(tCompile.get(i));
        }
        Database.memory.put(name, ret);
        return "";
    }

    static String printTable(String name) throws IOException {
        if (!Database.memory.containsKey(name)) {
            return ("Table " + name + " does not exist.");
        }
        Table ret = Database.memory.get(name);
        Table.rowNode ptr = ret.header;
        String returnString = "";
        for (int i = 0; i < ptr.vals.length - 1; i++) {
            returnString += ptr.vals[i] + ",";
        }
        returnString += ptr.vals[ptr.vals.length - 1] + "\n";
        for (int i = 0; i < ret.numRow; i++) {
            ptr = ptr.nextRow;
            for (int j = 0; j < ret.numCol; j++) {
                if (ret.header.vals[j].endsWith("string")) {
                    if (j == ret.numCol - 1) {
                        returnString += "'" + ptr.vals[j] + "'\n";
                    } else {
                        returnString += "'" + ptr.vals[j] + "',";
                    }
                } else if (ret.header.vals[j].endsWith("int")) {
                    if (j == ret.numCol - 1) {
                        returnString += ptr.vals[j] + "\n";
                    } else {
                        returnString += ptr.vals[j] + ",";
                    }
                } else {
                    if (j == ret.numCol - 1) {
                        returnString += ptr.vals[j] + "\n";
                    } else {
                        returnString += ptr.vals[j] + ",";
                    }
                }
            }
        }
        return returnString;
    }

    static String select(String expr) throws IOException {
        Matcher m = SELECT_CLS.matcher(expr);
        if (!m.matches()) {
            return ("Malformed select: %s\n" + expr);
        }
        return select(m.group(1), m.group(2), m.group(3));
    }

    static String select(String exprs, String tables, String conds) throws IOException {
        String[] tablesarr = tables.split(",");
        for (String t : tablesarr) {
            if (!Database.memory.containsKey(t)) {
                return ("Table " + t + " not in database.");
            }
        }
        Table joined = null;
        if (exprs.equals("*")) {
            if (tablesarr.length == 1) {
                Database.memory.get(tables);
            } else {
                Table tableA = Database.memory.get(tablesarr[0]);
                Table tableB = Database.memory.get(tablesarr[1]);
                joined = tableA.join(tableB);
                for (int i = 2; i < tablesarr.length; i++) {
                    Table nextT = Database.memory.get(tablesarr[i]);
                    joined = joined.join(nextT);
                }

            }
        }
        Table.rowNode ptr = joined.header;
        String returnString = "";
        for (int i = 0; i < ptr.vals.length - 1; i++) {
            returnString += ptr.vals[i] + ",";
        }
        returnString += ptr.vals[ptr.vals.length - 1] + "\n";
        for (int i = 0; i < joined.numRow; i++) {
            ptr = ptr.nextRow;
            for (int j = 0; j < joined.numCol; j++) {
                if (joined.header.vals[j].endsWith("string")) {
                    if (j == joined.numCol - 1) {
                        returnString += "'" + ptr.vals[j] + "'\n";
                    } else {
                        returnString += "'" + ptr.vals[j] + "',";
                    }
                } else if (joined.header.vals[j].endsWith("int")) {
                    if (j == joined.numCol - 1) {
                        returnString += ptr.vals[j] + "\n";
                    } else {
                        returnString += ptr.vals[j] + ",";
                    }
                } else {
                    if (j == joined.numCol - 1) {
                        returnString += ptr.vals[j] + "\n";
                    } else {
                        returnString += ptr.vals[j] + ",";
                    }
                }
            }
        }
        return returnString;
    }
}
