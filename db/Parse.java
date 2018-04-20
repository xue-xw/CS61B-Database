package db;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by White Noise on 2017/3/2.
 * And finished by XenonXue
 */

public class Parse {
    private static final String REST = "\\s*(.*)\\s*",
            COMMA = "\\s*,\\s*",
            AND = "\\s+and\\s+",
            STRING = "'.+'",
            INT = "-?\\d+",
            FLOAT = "-?\\d*\\.\\d*";

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
    private static final String[] Comparators = {"==", "!=", ">=", "<=", ">", "<"};
    private static final String[] ValidOperators = {"+", "-", "*", "/"};

    public static String parser(String[] args, Database db) throws IOException {
        if (args.length != 1) {
            return ("ERROR: Expected a single query argument");
        }
        return read(args[0], db);
    }

    private static String read(String query, Database db) throws IOException {
        Matcher m;
        if ((m = CREATE_CMD.matcher(query)).matches()) {
            return createTable(m.group(1), db);
        } else if ((m = LOAD_CMD.matcher(query)).matches()) {
            return loadTable(m.group(1), db);
        } else if ((m = STORE_CMD.matcher(query)).matches()) {
            return storeTable(m.group(1), db);
        } else if ((m = DROP_CMD.matcher(query)).matches()) {
            return dropTable(m.group(1), db);
        } else if ((m = INSERT_CMD.matcher(query)).matches()) {
            return insert(m.group(1), db);
        } else if ((m = PRINT_CMD.matcher(query)).matches()) {
            return printTable(m.group(1), db);
        } else if ((m = SELECT_CMD.matcher(query)).matches()) {
            return select(m.group(1), db);
        } else {
            return ("ERROR: Malformed query: " + query);
        }
    }

    private static String createTable(String expr, Database db) {
        Matcher m;
        if ((m = CREATE_NEW.matcher(expr)).matches()) {
            return createNewTable(m.group(1), m.group(2), db);
        } else if ((m = CREATE_SEL.matcher(expr)).matches()) {
            return createSelectedTable(m.group(1), m.group(2), m.group(3), m.group(4), db);
        } else {
            return ("ERROR: Malformed create: " + expr);
        }
    }

    private static String createNewTable(String name, String cols, Database db) {
        if (cols.isEmpty()) {
            return ("ERROR: null column input.");
        }
        if (db.memory.containsKey(name)) {
            return ("ERROR: table " + name + "is already in database.");
        }
        String[] nameAndType = cols.split(COMMA);
        String[] noS = new String[nameAndType.length];
        for (int i = 0; i < nameAndType.length; i++) {
            noS[i] = nameAndType[i].trim().replaceAll(" +", " ");
            if (!noS[i].matches("^[A-Za-z]\\w*\\s\\w+")) {
                return ("ERROR: column name " + noS[i] + " contains illegal character.");
            }
            if ((!noS[i].contains("int")) && (!noS[i].contains("float")) && (!noS[i].contains("string"))) {
                return ("ERROR: column name" + noS[i] + " has illegal typing.");
            }
        }

        Table ret = new Table(noS.length, noS);
        db.memory.put(name, ret);
        return "";
    }

    private static String createSelectedTable(String name, String exprs, String tables, String conds, Database db) {
        if (db.memory.containsKey(name)) {
            return ("ERROR: table " + name + "already in database.");
        }
        String selected = select(exprs, tables, conds, db);
        Scanner sc = new Scanner(selected);
        String[] header = sc.nextLine().split(COMMA);
        Table ret = new Table(header.length, header);
        String[] row;
        while (sc.hasNext()) {
            row = sc.nextLine().split(COMMA);
            for (int i = 0; i < row.length; i++) {
                if (header[i].contains("string")) {
                    if (row[i].equals("NaN") || row[i].equals("NOVALUE")) {
                        continue;
                    }
                    if (isQuoted(row[i])) {
                        row[i] = unQuote(row[i]);
                    }
                }
            }
            ret.addRow(row);
        }
        sc.close();
        db.memory.put(name, ret);

        return "";
    }


    private static String loadTable(String name, Database db) throws IOException {
        try {
            BufferedReader tableFile = new BufferedReader(new FileReader(name + ".tbl"));
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException("ERROR: " + name + ".tbl is not found");
        }
        BufferedReader tableFile = new BufferedReader(new FileReader(name + ".tbl"));
        String firstSentence = tableFile.readLine();
        if (firstSentence == null) {
            return ("ERROR: " + name + ".tbl is an empty file.");
        }
        //processing the header
        String[] colNames = firstSentence.split(",");
        for (String word : colNames) {  //check header is correctly formatted
            if (!word.matches("^[A-Za-z]\\w*\\s\\w+")) {
                return ("ERROR: column name " + word + " contains illegal character.");
            }
            if ((!word.contains("int")) && (!word.contains("float")) && (!word.contains("string"))) {
                return ("ERROR: column name" + word + " has illegal typing.");
            }
        }
        //processing the value rows
        Table ret = new Table(colNames.length, colNames);
        String sentence = tableFile.readLine();
        while (sentence != null) {
            String[] row = sentence.split(",");
            if (colNames.length != row.length) {
                return ("ERROR: the number of values in a certain row does not match the column number.");
            }
            for (int i = 0; i < row.length; i++) {
                if (row[i].equals("NaN") || row[i].equals("NOVALUE")) {
                    continue;
                }
                if ((colNames[i].contains("string")) && ((!(isQuoted(row[i]))) || (!row[i].matches(STRING)))) {
                    return ("ERROR: value " + row[i] + " is not a string");
                }
                if ((colNames[i].contains("int")) && (!row[i].matches(INT))) {
                    return ("ERROR: value " + row[i] + " is not an int.");
                }
                if ((colNames[i].contains("float") && (!row[i].matches(FLOAT)))) {
                    return ("ERROR: value " + row[i] + " is not a float.");
                }
            }
            for (int i = 0; i < row.length; i++) {
                if (colNames[i].contains("string")) {
                    if (isQuoted(row[i])) {
                        row[i] = unQuote(row[i]);
                    }
                }
            }
            ret.addRow(row);
            sentence = tableFile.readLine();
        }
        tableFile.close();
        if (db.memory.containsKey(name)) {
            db.memory.replace(name, ret);
        } else {
            db.memory.put(name, ret);
        }
        return "";
    }

    private static String storeTable(String name, Database db) throws IOException {
        if (!db.memory.containsKey(name)) {
            return ("ERROR: table " + name + " is not in database.");
        }
        Table obj = db.memory.get(name);
        FileWriter t = new FileWriter(name + ".tbl");
        BufferedWriter out = new BufferedWriter(t);

        String header = Arrays.toString(obj.header.vals).replaceAll("^\\[", "").replaceAll("]$", "")
                .replaceAll(COMMA, ",");
        out.write(header);
        out.newLine();

        StringJoiner comma = new StringJoiner(",");
        Table.rowNode ptr = obj.header.nextRow;
        String[] topRow = obj.header.vals;
        String output;
        while (ptr != null) {
            for (int i = 0; i < topRow.length; i++) {
                if (topRow[i].contains("string") && !ptr.vals[i].equals("NOVALUE") && !ptr.vals[i].equals("NaN")) {
                    output = "'" + ptr.vals[i] + "'";
                } else {
                    output = ptr.vals[i];
                }
                comma.add(output);
            }
            out.write(comma.toString());
            out.newLine();
            comma = new StringJoiner(",");
            ptr = ptr.nextRow;
        }
        out.close();
        return "";
    }

    private static String dropTable(String name, Database db) {
        if (!db.memory.containsKey(name)) {
            return ("ERROR: table " + name + "is not in database.");
        }
        db.memory.remove(name);
        return "";
    }

    private static String insert(String expr, Database db) {
        Matcher m = INSERT_CLS.matcher(expr);
        if (!m.matches()) {
            return ("ERROR: Malformed create: " + expr);
        }
        return insertRow(m.group(1), m.group(2), db);
    }

    private static String insertRow(String name, String values, Database db) {
        if (!db.memory.containsKey(name)) {
            return ("ERROR: table " + name + " is not in database.");
        }
        Table obj = db.memory.get(name);

        String[] vals = values.split(COMMA);
        if (obj.numCol != vals.length) {
            return ("ERROR: the number of insert values is incorrect.");
        }
        for (int i = 0; i < vals.length; i++) {
            if (vals[i].equals("NaN") || vals[i].equals("NOVALUE")) {
                continue;
            }
            if (obj.header.vals[i].contains("string") && (!vals[i].matches(STRING))) {
                return ("ERROR: value " + vals[i] + " is not a string");
            }
            if (obj.header.vals[i].contains("int") && (!vals[i].matches(INT))) {
                return ("ERROR: value " + vals[i] + " is not an int");
            }
            if ((obj.header.vals[i].contains("float") && (!vals[i].matches(FLOAT)))) {
                return ("ERROR: value " + vals[i] + " is not a float");
            }
        }

        for (int i = 0; i < vals.length; i++) {
            if (obj.header.vals[i].contains("string")) {
                if (vals[i].equals("NOVALUE")) {
                    continue;
                }
                vals[i] = unQuote(vals[i]);
            }
            if (obj.header.vals[i].contains("float")) {
                if (vals[i].equals("NOVALUE")) {
                    continue;
                }
                vals[i] = String.format("%.3f", Float.parseFloat(vals[i]));
            }
        }
        obj.addRow(vals);
        db.memory.replace(name, obj);
        return "";
    }

    static String getReturnString(Table ret) {
        Table.rowNode ptr = ret.header;
        StringBuilder returnString = new StringBuilder();
        for (int i = 0; i < ptr.vals.length - 1; i++) {
            returnString.append(ptr.vals[i]);
            returnString.append(",");
        }
        returnString.append(ptr.vals[ptr.vals.length - 1]);
        returnString.append("\n");

        for (int i = 0; i < ret.numRow; i++) {
            ptr = ptr.nextRow;
            for (int j = 0; j < ret.numCol; j++) {

                if (ptr.vals[j].equals("NaN") || ptr.vals[j].equals("NOVALUE")) {
                    if (j == ret.numCol - 1) {
                        returnString.append(ptr.vals[j]);
                        returnString.append("\n");
                    } else {
                        returnString.append(ptr.vals[j]);
                        returnString.append(",");
                    }
                    continue;
                }

                if (ret.header.vals[j].endsWith("string")) {
                    if (j == ret.numCol - 1) {
                        returnString.append("'");
                        returnString.append(ptr.vals[j]);
                        returnString.append("'");
                        returnString.append("\n");
                    } else {
                        returnString.append("'");
                        returnString.append(ptr.vals[j]);
                        returnString.append("'");
                        returnString.append(",");
                    }
                } else if (ret.header.vals[j].endsWith("int")) {
                    if (j == ret.numCol - 1) {
                        returnString.append(ptr.vals[j]);
                        returnString.append("\n");
                    } else {
                        returnString.append(ptr.vals[j]);
                        returnString.append(",");
                    }
                } else {
                    if (j == ret.numCol - 1) {
                        returnString.append(String.format("%.3f", Float.parseFloat(ptr.vals[j])));
                        returnString.append("\n");
                    } else {
                        returnString.append(String.format("%.3f", Float.parseFloat(ptr.vals[j])));
                        returnString.append(",");
                    }
                }

            }
        }
        return returnString.toString();
    }

    static String printTable(String name, Database db) {
        if (!db.memory.containsKey(name)) {
            return ("ERROR: Table " + name + " does not exist.");
        }
        Table ret = db.memory.get(name);
        return getReturnString(ret);

    }

    static String select(String expr, Database db) throws IOException {
        Matcher m = SELECT_CLS.matcher(expr);
        if (!m.matches()) {
            return ("ERROR: Malformed select:" + expr);
        }

        return select(m.group(1), m.group(2), m.group(3), db);
    }

    static String select(String exprs, String tables, String conds, Database db) {
        Table joined;

        // Reformat everything
        exprs = exprs.replaceAll("\\s+", " ");
        exprs = exprs.replaceAll("\\s,\\s", ",");
        exprs = exprs.replaceAll(",\\s", ",");
        exprs = exprs.replaceAll("\\s,", ",");
        String[] exprsArr = exprs.split(",");

        tables = tables.replaceAll("\\s+", "");
        String[] tablesArr = tables.split(",");


        // check tables in db
        for (String t : tablesArr) {
            if (!db.memory.containsKey(t)) {
                return ("ERROR: Table " + t + " not in database.");
            }
        }

        // join & evaluate exprs
        joined = db.memory.get(tablesArr[0]);
        for (int i = 1; i < tablesArr.length; i++) {
            joined = joined.join(db.memory.get(tablesArr[i]));
        }

        // Maps header to index in Table joined
        HashMap<String, Integer> colIndex = new HashMap<>();
        for (int i = 0; i < joined.numCol; i++) {
            colIndex.put(joined.header.vals[i].substring(0, joined.header.vals[i].indexOf(" ")), i);
        }

        Table alias = joined.makeCopy();
        alias.numCol = 0;
        alias.numRow = joined.numRow;

        if (!exprs.equals("*")) {
            for (String exp : exprsArr) {
                if (exp.contains(" as ")) {
                    int pos = exp.indexOf(" as ");
                    String aliasColumnName = exp.substring(pos + 4);
                    String operation = exp.substring(0, pos);
                    operation = operation.replaceAll("\\s", "");
                    // TODO: check aliasColumnName is a valid name

                    String opType = getOperatorType(operation);
                    if (opType.equals("Bad Type")) {
                        return "ERROR: Operator not allowed.";
                    }
                    pos = operation.indexOf(opType);
                    String op1 = operation.substring(0, pos);
                    String op2 = operation.substring(pos + 1);

                    // check op1 is a column name
                    if (!joined.colType.containsKey(op1)) {
                        return "ERROR: Selecting column not exist.";
                    }
                    String type1 = joined.colType.get(op1);
                    int ind1 = colIndex.get(op1);

                    String[] newCol = new String[joined.numRow + 1];

                    if (getType(op2, colIndex).equals("literal int") || getType(op2, colIndex).equals("literal float")) {
                        // Unary operation with a number
                        // A + 10   '25' + 10 --> '35'
                        if (type1.equals("int") || type1.equals("float")) {

                            String type2 = getType(op2, colIndex).substring(getType(op2, colIndex).indexOf(" ") + 1);
                            String[] col1 = joined.getColVal(ind1);
                            if (type1.equals("int") && type2.equals("int")) {
                                // Int column & Int literal
                                newCol[0] = aliasColumnName + " int";
                                for (int i = 0; i < joined.numRow; i++) {
                                    if (col1[i].equals("NOVALUE")) {
                                        col1[i] = "0";
                                    }
                                }
                                if (opType.equals("+")) {
                                    for (int i = 1; i <= joined.numRow; i++) {
                                        if (col1[i - 1].equals("NaN")) {
                                            newCol[i] = "NaN";
                                            continue;
                                        }
                                        newCol[i] = Integer.toString(Integer.valueOf(col1[i - 1]) + Integer.valueOf(op2));
                                    }
                                } else if (opType.equals("-")) {
                                    for (int i = 1; i <= joined.numRow; i++) {
                                        if (col1[i - 1].equals("NaN")) {
                                            newCol[i] = "NaN";
                                            continue;
                                        }
                                        newCol[i] = Integer.toString(Integer.valueOf(col1[i - 1]) - Integer.valueOf(op2));
                                    }
                                } else if (opType.equals("*")) {
                                    for (int i = 1; i <= joined.numRow; i++) {
                                        if (col1[i - 1].equals("NaN")) {
                                            newCol[i] = "NaN";
                                            continue;
                                        }
                                        newCol[i] = Integer.toString(Integer.valueOf(col1[i - 1]) * Integer.valueOf(op2));
                                    }
                                } else if (opType.equals("/")) {
                                    for (int i = 1; i <= joined.numRow; i++) {
                                        if (Float.valueOf(op2) == 0) {
                                            newCol[i] = "NaN";
                                            continue;
                                        }
                                        if (col1[i - 1].equals("NaN")) {
                                            newCol[i] = "NaN";
                                            continue;
                                        }
                                        newCol[i] = Integer.toString(Integer.valueOf(col1[i - 1]) / Integer.valueOf(op2));
                                    }
                                }
                            } else if (type1.equals("int") && type2.equals("float") ||
                                    (type1.equals("float") && type2.equals("int")) || (type1.equals("float") && type2.equals("float"))) {
                                // Column & Literal = Float
                                newCol[0] = aliasColumnName + " float";
                                for (int i = 0; i < joined.numRow; i++) {
                                    if (col1[i].equals("NOVALUE")) {
                                        col1[i] = "0.0";
                                    }
                                }
                                if (opType.equals("+")) {
                                    for (int i = 1; i <= joined.numRow; i++) {
                                        if (col1[i - 1].equals("NaN")) {
                                            newCol[i] = "NaN";
                                            continue;
                                        }
                                        newCol[i] = Float.toString(Float.valueOf(col1[i - 1]) + Float.valueOf(op2));
                                    }
                                } else if (opType.equals("-")) {
                                    for (int i = 1; i <= joined.numRow; i++) {
                                        if (col1[i - 1].equals("NaN")) {
                                            newCol[i] = "NaN";
                                            continue;
                                        }
                                        newCol[i] = Float.toString(Float.valueOf(col1[i - 1]) - Float.valueOf(op2));
                                    }
                                } else if (opType.equals("*")) {
                                    for (int i = 1; i <= joined.numRow; i++) {
                                        if (col1[i - 1].equals("NaN")) {
                                            newCol[i] = "NaN";
                                            continue;
                                        }
                                        newCol[i] = Float.toString(Float.valueOf(col1[i - 1]) * Float.valueOf(op2));
                                    }
                                } else if (opType.equals("/")) {
                                    for (int i = 1; i <= joined.numRow; i++) {
                                        if (Float.valueOf(op2) == 0) {
                                            newCol[i] = "NaN";
                                            continue;
                                        }
                                        if (col1[i - 1].equals("NaN")) {
                                            newCol[i] = "NaN";
                                            continue;
                                        }
                                        newCol[i] = Float.toString(Float.valueOf(col1[i - 1]) / Float.valueOf(op2));
                                    }
                                }

                            }
                        } else {
                            return "ERROR: Type does not match operation with number.";
                        }

                    } else if (getType(op2, colIndex).equals("literal string")) {
                        // Unary operation with a string - Concatenation
                        if (!type1.equals("string")) {
                            return "ERROR: Wrong type operation. Trying to join nonstring with string.";
                        }
                        if (opType.equals("+")) {
                            op2 = op2.substring(1, op2.length() - 1);
                            String[] col1 = joined.getColVal(ind1);
                            newCol[0] = aliasColumnName + " string";
                            for (int i = 1; i <= joined.numRow; i++) {
                                if (col1[i - 1].equals("NaN")) {
                                    newCol[i] = "NaN";
                                    continue;
                                }
                                if (col1[i - 1].equals("NOVALUE")) {
                                    newCol[i] = op2;
                                    continue;
                                }
                                newCol[i] = col1[i - 1] + op2;
                            }
                        } else {
                            return "ERROR: String operation not allowed. String only allows concatenation.";
                        }

                    } else if (getType(op2, colIndex).equals("column")) {

                        // Binary Operations
                        if (!joined.colType.containsKey(op2)) {
                            return "ERROR: Selecting column not exist.";
                        }

                        int ind2 = colIndex.get(op2);
                        String type2 = joined.colType.get(op2);
                        String[] col1 = joined.getColVal(ind1);
                        String[] col2 = joined.getColVal(ind2);
                        if (type1.equals("int") && type2.equals("int")) {
                            newCol[0] = aliasColumnName + " int";

                            for (int i = 0; i < joined.numRow; i++) {
                                if (col1[i].equals("NOVALUE")) {
                                    col1[i] = "0";
                                }
                                if (col2[i].equals("NOVALUE")) {
                                    col2[i] = "0";
                                }
                            }

                            if (opType.equals("+")) {
                                for (int i = 1; i <= joined.numRow; i++) {
                                    if (col1[i - 1].equals("NaN") || col2[i - 1].equals("NaN")) {
                                        newCol[i] = "NaN";
                                        continue;
                                    }
                                    newCol[i] = Integer.toString(Integer.valueOf(col1[i - 1]) + Integer.valueOf(col2[i - 1]));
                                }
                            } else if (opType.equals("-")) {
                                for (int i = 1; i <= joined.numRow; i++) {
                                    if (col1[i - 1].equals("NaN") || col2[i - 1].equals("NaN")) {
                                        newCol[i] = "NaN";
                                        continue;
                                    }
                                    newCol[i] = Integer.toString(Integer.valueOf(col1[i - 1]) - Integer.valueOf(col2[i - 1]));
                                }
                            } else if (opType.equals("*")) {
                                for (int i = 1; i <= joined.numRow; i++) {
                                    if (col1[i - 1].equals("NaN") || col2[i - 1].equals("NaN")) {
                                        newCol[i] = "NaN";
                                        continue;
                                    }
                                    newCol[i] = Integer.toString(Integer.valueOf(col1[i - 1]) * Integer.valueOf(col2[i - 1]));
                                }
                            } else if (opType.equals("/")) {
                                for (int i = 1; i <= joined.numRow; i++) {
                                    if (col1[i - 1].equals("NaN") || col2[i - 1].equals("NaN")) {
                                        newCol[i] = "NaN";
                                        continue;
                                    }
                                    if (Float.valueOf(col2[i - 1]) == 0) {
                                        newCol[i] = "NaN";
                                        continue;
                                    }
                                    newCol[i] = Integer.toString(Integer.valueOf(col1[i - 1]) / Integer.valueOf(col2[i - 1]));
                                }
                            }
                        } else if (type1.equals("int") && type2.equals("float") ||
                                (type1.equals("float") && type2.equals("int")) || (type1.equals("float") && type2.equals("float"))) {
                            newCol[0] = aliasColumnName + " float";
                            for (int i = 0; i < joined.numRow; i++) {
                                if (col1[i].equals("NOVALUE")) {
                                    col1[i] = "0.0";
                                }
                                if (col2[i].equals("NOVALUE")) {
                                    col2[i] = "0.0";
                                }
                            }
                            if (opType.equals("+")) {
                                for (int i = 1; i <= joined.numRow; i++) {
                                    if (col1[i - 1].equals("NaN") || col2[i - 1].equals("NaN")) {
                                        newCol[i] = "NaN";
                                        continue;
                                    }
                                    newCol[i] = Float.toString(Float.valueOf(col1[i - 1]) + Float.valueOf(col2[i - 1]));
                                }
                            } else if (opType.equals("-")) {
                                for (int i = 1; i <= joined.numRow; i++) {
                                    if (col1[i - 1].equals("NaN") || col2[i - 1].equals("NaN")) {
                                        newCol[i] = "NaN";
                                        continue;
                                    }
                                    newCol[i] = Float.toString(Float.valueOf(col1[i - 1]) - Float.valueOf(col2[i - 1]));
                                }
                            } else if (opType.equals("*")) {
                                for (int i = 1; i <= joined.numRow; i++) {
                                    if (col1[i - 1].equals("NaN") || col2[i - 1].equals("NaN")) {
                                        newCol[i] = "NaN";
                                        continue;
                                    }
                                    newCol[i] = Float.toString(Float.valueOf(col1[i - 1]) * Float.valueOf(col2[i - 1]));
                                }
                            } else if (opType.equals("/")) {
                                for (int i = 1; i <= joined.numRow; i++) {
                                    if (col1[i - 1].equals("NaN") || col2[i - 1].equals("NaN")) {
                                        newCol[i] = "NaN";
                                        continue;
                                    }
                                    if (Float.valueOf(col2[i - 1]) == 0) {
                                        newCol[i] = "NaN";
                                        continue;
                                    }
                                    newCol[i] = Float.toString(Float.valueOf(col1[i - 1]) / Float.valueOf(col2[i - 1]));
                                }
                            }
                        } else if (type1.equals("string") && type2.equals("string")) {
                            newCol[0] = aliasColumnName + " string";
                            for (int i = 0; i < joined.numRow; i++) {
                                if (col1[i].equals("NOVALUE")) {
                                    col1[i] = "";
                                }
                                if (col2[i].equals("NOVALUE")) {
                                    col2[i] = "";
                                }
                            }
                            if (opType.equals("+")) {
                                for (int i = 1; i <= joined.numRow; i++) {
                                    if (col1[i - 1].equals("NaN") || col2[i - 1].equals("NaN")) {
                                        newCol[i] = "NaN";
                                        continue;
                                    }
                                    newCol[i] = col1[i - 1] + col2[i - 1];
                                }
                            } else {
                                return "ERROR: illegal string operation.";
                            }
                        } else {
                            return "ERROR: illegal type in operation.";
                        }

                    } else if (getType(op2, colIndex).equals("Invalid type")) {
                        return "ERROR: Invalid operand.";
                    }

                    alias = alias.addColumn(newCol);

                } else {
                    // Keep the original column name
                    if (!joined.colType.containsKey(exp)) {
                        return "ERROR: column selected does not exist in tables.";
                    }
                    String[] newCol = new String[joined.numRow + 1];
                    newCol[0] = exp + " " + joined.colType.get(exp);
                    System.arraycopy(joined.getColVal(colIndex.get(exp)), 0, newCol, 1, joined.numRow);
                    alias = alias.addColumn(newCol);
                }
            } // for every exp, do the above
        } else {
            alias = joined.makeCopy();
        }

        colIndex.clear();
        for (int i = 0; i < alias.numCol; i++) {
            colIndex.put(alias.header.vals[i].substring(0, alias.header.vals[i].indexOf(" ")), i);
        }


        // ============================================================================================
        // conditions exist, then prune out rows in Table alias
        if (conds != null) {
            conds = conds.replaceAll("\\s+and\\s+", ",");
            conds = conds.replaceAll("\\s+", "");
            String[] conditionArr = conds.split(",");

            /**System.out.println(conditionArr[0]);
             System.out.println(conditionArr[1]);
             System.out.println(conditionArr[2]);*/

            for (String c : conditionArr) {
                String comp = getComparatorType(c);
                if (comp.equals("Bad Type")) {
                    return "ERROR: Comparator Bad Type.";
                }
                int pos = c.indexOf(comp);
                String op1 = c.substring(0, pos);
                String op2 = c.substring(pos + comp.length());

                String opType1 = getType(op1, colIndex);
                String opType2 = getType(op2, colIndex);

                // op1 valid column
                if (!opType1.equals("column") || !alias.colType.containsKey(op1)) {
                    return "ERROR: Invalid condition.";
                }

                if (opType2.equals("column")) {
                    int ind1 = colIndex.get(op1);
                    int ind2 = colIndex.get(op2);
                    String colType1 = alias.colType.get(op1);
                    String colType2 = alias.colType.get(op2);

                    // Both op1&op2 are columns

                    if (colType1.equals("string") && !colType2.equals("string")) {
                        return "ERROR: String cannot compare with nonstring.";
                    }
                    if (colType2.equals("string") && !colType1.equals("string")) {
                        return "ERROR: String cannot compare with nonstring.";
                    }

                    if (colType1.equals("int") || colType1.equals("float")) {
                        // Two number columns

                        // Inspect every row under this condition: c
                        Table.rowNode p = alias.header.nextRow;
                        Table.rowNode prev = alias.header;
                        while (p != null) {
                            String val1 = p.vals[ind1];
                            String val2 = p.vals[ind2];
                            if (!compareFloat(val1, val2, comp)) {
                                // Delete this row
                                prev.nextRow = p.nextRow;
                                p = p.nextRow;
                                alias.numRow--;
                            } else {
                                // Keep this row
                                prev = p;
                                p = p.nextRow;
                            }
                        }
                    } else {
                        // Two string columns

                        // Inspect every row under this condition: c
                        Table.rowNode p = alias.header.nextRow;
                        Table.rowNode prev = alias.header;
                        while (p != null) {
                            String val1 = p.vals[ind1];
                            String val2 = p.vals[ind2];
                            if (!compareString(val1, val2, comp)) {
                                // Delete this row
                                prev.nextRow = p.nextRow;
                                p = p.nextRow;
                                alias.numRow--;
                            } else {
                                // Keep this row
                                prev = p;
                                p = p.nextRow;
                            }
                        }
                    }

                } else if (opType2.equals("literal int") || opType2.equals("literal float")) {
                    // Number column compare with literal number

                    String colType1 = alias.colType.get(op1);
                    if (!(colType1.equals("int") || colType1.equals("float"))) {
                        return "ERROR: Trying to compare String with Number.";
                    }

                    int ind1 = colIndex.get(op1);


                    // Inspect every row under this condition: c
                    Table.rowNode p = alias.header.nextRow;
                    Table.rowNode prev = alias.header;
                    while (p != null) {
                        String val1 = p.vals[ind1];
                        if (!compareFloat(val1, op2, comp)) {
                            // Delete this row
                            prev.nextRow = p.nextRow;
                            p = p.nextRow;
                            alias.numRow--;
                        } else {
                            // Keep this row
                            prev = p;
                            p = p.nextRow;
                        }
                    }
                } else if (opType2.equals("literal string")) {
                    // String column compare with literal string
                    op2 = unQuote(op2);
                    String colType1 = alias.colType.get(op1);
                    if (!colType1.equals("string")) {
                        return "ERROR: Trying to compare Number with String.";
                    }
                    int ind1 = colIndex.get(op1);

                    // Inspect every row under this condition: c
                    Table.rowNode p = alias.header.nextRow;
                    Table.rowNode prev = alias.header;
                    while (p != null) {
                        String val1 = p.vals[ind1];
                        if (!compareString(val1, op2, comp)) {
                            // Delete this row
                            prev.nextRow = p.nextRow;
                            p = p.nextRow;
                            alias.numRow--;
                        } else {
                            // Keep this row
                            prev = p;
                            p = p.nextRow;
                        }
                    }
                } else {
                    return "ERROR: Invalid condition.";
                }
            }
        }


        return getReturnString(alias);
    }

    private static String getComparatorType(String s) {
        for (String comp : Comparators) {
            if (s.contains(comp)) {
                return comp;
            }
        }
        return "Bad Type";
    }

    private static String getOperatorType(String s) {
        for (String op : ValidOperators) {
            if (s.contains(op)) {
                return op;
            }
        }
        return "Bad Type";
    }

    private static String getType(String s, HashMap<String, Integer> colIndex) {
        if (s.matches(INT)) {
            return "literal int";
        } else if (s.matches(FLOAT)) {
            return "literal float";
        } else if (s.matches(STRING)) {
            return "literal string";
        } else if (colIndex.containsKey(s)) {
            return "column";
        }
        return "Invalid type";
    }

    private static String unQuote(String s) {
        return s.substring(1, s.length() - 1);
    }

    private static boolean isQuoted(String s) {
        return s.startsWith("'") && s.endsWith("'");
    }

    private static boolean compareFloat(String s1, String s2, String comp) {
        if (s1.equals("NaN")) {
            if (s2.equals("NaN")) {
                return comp.equals("==");
            } else {
                return comp.equals("!=") || comp.equals(">") || comp.equals(">=");
            }
        }
        if (s2.equals("NaN")) {
            return comp.equals("!=") || comp.equals("<") || comp.equals("<=");
        }
        float a, b;
        if (s1.equals("NOVALUE")) {
            return false;
        } else {
            a = Float.valueOf(s1);
        }
        if (s2.equals("NOVALUE")) {
            return false;
        } else {
            b = Float.valueOf(s2);
        }
        if (comp.equals("==")) {
            return a == b;
        }
        if (comp.equals("!=")) {
            return a != b;
        }
        if (comp.equals(">")) {
            return a > b;
        }
        if (comp.equals("<")) {
            return a < b;
        }
        if (comp.equals(">=")) {
            return a >= b;
        }
        if (comp.equals("<=")) {
            return a <= b;
        }
        return true;
    }

    private static boolean compareString(String s1, String s2, String comp) {
        if (s1.equals("NaN")) {
            if (s2.equals("NaN")) {
                return comp.equals("==");
            } else {
                return comp.equals("!=") || comp.equals(">") || comp.equals(">=");
            }
        }
        if (s2.equals("NaN")) {
            return comp.equals("!=") || comp.equals("<") || comp.equals("<=");
        }
        String a, b;
        if (s1.equals("NOVALUE")) {
            return false;
        } else {
            a = s1;
        }
        if (s2.equals("NOVALUE")) {
            return false;
        } else {
            b = s2;
        }
        if (comp.equals("==")) {
            return a.equals(b);
        }
        if (comp.equals("!=")) {
            return !a.equals(b);
        }
        if (comp.equals(">")) {
            return a.compareTo(b) > 0;
        }
        if (comp.equals("<")) {
            return a.compareTo(b) < 0;
        }
        if (comp.equals(">=")) {
            return a.compareTo(b) > 0 || a.compareTo(b) == 0;
        }
        if (comp.equals("<=")) {
            return a.compareTo(b) < 0 || a.compareTo(b) == 0;
        }
        return true;
    }
}
