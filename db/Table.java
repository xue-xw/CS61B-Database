package db;

import java.io.IOException;
import java.util.*;

/**
 * Created by White Noise on 2017/2/21.
 * But completed by XenonXue 2017/3/6.
 */
public class Table {
    rowNode header;
    int numRow;
    int numCol;
    rowNode last;
    HashMap<String, String> colType = new HashMap<>(); // Map header to type

    protected class rowNode {
        rowNode nextRow;
        String[] vals;

        rowNode(int elems) {
            vals = new String[elems];
        }
    }

    public Table(int numCol, String[] topRow) {
        header = new rowNode(numCol);
        header.vals = topRow;
        this.numCol = numCol;
        numRow = 0;
        last = header;
        for (String col : topRow) {
            int pos = col.indexOf(' ');
            colType.put(col.substring(0, pos), col.substring(pos + 1));
        }
    }

    public void addRow(String[] given) {
        numRow += 1;
        rowNode curRow = new rowNode(numCol);
        curRow.vals = given;
        this.last.nextRow = curRow;
        this.last = curRow;
    }

    public Table addColumn(String[] given) {
        String[] newHeader = new String[numCol + 1];
        System.arraycopy(header.vals, 0, newHeader, 0, numCol);
        newHeader[numCol] = given[0];
        Table ret = new Table(numCol + 1, newHeader);
        ret.numRow = numRow;
        if (this.numCol == 0) {
            rowNode prevRow = ret.header;
            for (int i = 1; i <= numRow; i++) {
                rowNode newRow = new rowNode(ret.numCol);
                newRow.vals[0] = given[i];
                prevRow.nextRow = newRow;
                prevRow = newRow;
            }
            return ret;
        }
        rowNode p = this.header.nextRow;
        rowNode q = ret.header;
        for (int i = 1; i <= numRow; i++) {
            rowNode newRow = new rowNode(ret.numCol);
            newRow.vals[numCol] = given[i];
            System.arraycopy(p.vals, 0, newRow.vals, 0, numCol);
            q.nextRow = newRow;
            q = q.nextRow;
            p = p.nextRow;
        }
        return ret;
    }

    public String[] getColVal(int index) {
        String[] ret = new String[numRow];
        rowNode ptr = this.header.nextRow;
        for (int i = 0; i < this.numRow; i++) {
            ret[i] = ptr.vals[index];
            ptr = ptr.nextRow;
        }
        return ret;
    }

    public Table join(Table b) {
        // Make 2 copies, and prune rows from them
        Set<String> colNameA = new HashSet<>(Arrays.asList(header.vals));
        Set<String> copyOfcolNameA = new HashSet<>(Arrays.asList(header.vals));
        Set<String> colNameB = new HashSet<>(Arrays.asList(b.header.vals));

        // colNameA : common column names
        for (String i : copyOfcolNameA) {
            if (!colNameB.contains(i)) {
                colNameA.remove(i);
            }
        }
        if (colNameA.size() == 0) {
            return this.cartesianProd(b);
        } else {
            // For tables with common cols:
            // Erase all rows that do not participate in the join
            Table copyA = this.makeCopy();
            Table copyB = b.makeCopy();
            for (int i = 0; i < copyA.numCol; i++) {
                for (int j = 0; j < copyB.numCol; j++) {
                    if (copyA.header.vals[i].equals(copyB.header.vals[j])) {   // Common Column
                        int colAIndex = i;
                        int colBIndex = j;
                        Set<String> colASet = new HashSet<>(Arrays.asList(copyA.getColVal(i)));
                        Set<String> colBSet = new HashSet<>(Arrays.asList(copyB.getColVal(j)));
                        copyA.rowEraser(i, colBSet);
                        copyB.rowEraser(j, colASet);
                        break;
                    }
                }
            }

            // At this stage, all rows we have need to be joined
            // Generate the header of ret
            int numCommon = colNameA.size();
            String[] retHeader = new String[copyA.numCol + copyB.numCol - numCommon];

            int p = -1, q = numCommon - 1;
            for (String i : copyA.header.vals) {
                if (colNameA.contains(i)) {
                    p++;
                    retHeader[p] = i;
                } else {
                    q++;
                    retHeader[q] = i;
                }
            }
            for (String i : copyB.header.vals) {
                if (!colNameA.contains(i)) {
                    q++;
                    retHeader[q] = i;
                }
            }
            Table ret = new Table(copyA.numCol + copyB.numCol - numCommon, retHeader);


            // Map all retTable colNames to index
            HashMap<String, Integer> colNameInd = new HashMap<>();
            for (int i = 0; i < ret.numCol; i++) {
                colNameInd.put(ret.header.vals[i], i);
            }

            // Cartesian Join two tables
            Table messyJoin = copyA.cartesianProd(copyB);

            // Prune out useless rows and copy into ret
            rowNode rowPointer = messyJoin.header.nextRow;
            for (int k = 1; k <= messyJoin.numRow; k++) {  // Inspect every row
                rowNode curRow = new rowNode(ret.numCol); // Prepare to generate a pruned clean joined row

                boolean isBadRow = false;

                Find_Bad_Row:

                for (int i = 0; i < copyA.numCol; i++) {
                    for (int j = i + 1; j < messyJoin.numCol; j++) {
                        if (messyJoin.header.vals[i].equals
                                (messyJoin.header.vals[j])) {// Find if it has two same columns


                            if (!rowPointer.vals[i].equals
                                    (rowPointer.vals[j])) {
                                isBadRow = true;
                                break Find_Bad_Row;   // Skip this row
                            }
                        }
                    }
                }

                if (!isBadRow) {
                    for (int i = 0; i < messyJoin.numCol; i++) {
                        int pos = colNameInd.get(messyJoin.header.vals[i]);
                        curRow.vals[pos] = rowPointer.vals[i];
                    }
                    ret.addRow(curRow.vals);
                }
                rowPointer = rowPointer.nextRow;
            }

            return ret;
        }
    }

    private Table cartesianProd(Table b) {    // BY PYR
        String[] newHeader = new String[this.numCol + b.numCol];
        System.arraycopy(this.header.vals, 0, newHeader, 0, this.numCol);
        System.arraycopy(b.header.vals, 0, newHeader, this.numCol, b.numCol);
        Table ret = new Table(this.numCol + b.numCol, newHeader);

        rowNode p = this.header.nextRow, q = b.header.nextRow;
        for (int i = 1; i <= this.numRow; i++) {    //Cartesian join
            for (int j = 1; j <= b.numRow; j++) {
                String[] curRow = new String[ret.numCol];
                System.arraycopy(p.vals, 0, curRow, 0, this.numCol);
                System.arraycopy(q.vals, 0, curRow, this.numCol, b.numCol);
                ret.addRow(curRow);
                q = q.nextRow;
            }
            p = p.nextRow;
            q = b.header.nextRow;
        }
        return ret;
    }

    public void rowEraser(int i, Set<String> setB) {  // BY PYR
        rowNode backPtr = this.header;
        rowNode frontPtr = this.header.nextRow;
        while (frontPtr != null) {
            if (!setB.contains(frontPtr.vals[i])) {
                backPtr.nextRow = frontPtr.nextRow;
                frontPtr = frontPtr.nextRow;
                this.numRow -= 1;
            } else {
                frontPtr = frontPtr.nextRow;
                backPtr = backPtr.nextRow;
            }
        }
    }

    public Table makeCopy() {
        Table a = new Table(this.numCol, this.header.vals);
        rowNode p = this.header.nextRow;
        rowNode q = a.header;
        for (int i = 1; i <= this.numRow; i++) {
            rowNode r = new rowNode(this.numCol);
            System.arraycopy(p.vals, 0, r.vals, 0, this.numCol);
            a.numRow++;
            q.nextRow = r;
            p = p.nextRow;
            q = q.nextRow;
        }
        return a;

    }
}

