package db;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by White Noise on 2017/2/22.
 */
public class Database {
    HashMap<String, Table> memory;

    public Database() {
        memory = new HashMap<>();
    }

    public String transact(String query) {
        String[] queries = new String[1];
        queries[0] = query;
        try {
            return Parse.parser(queries, this);
        } catch (IOException e) {
            return ("ERROR: Illegal transaction.");
        }
    }

    public static void main(String[] args) throws IOException {
        Database db = new Database();
        String line = "load t1";
        System.out.print(db.transact(line));

        line = "load t4";
        System.out.print(db.transact(line));
        line = "insert into t4 values NOVALUE,2";
        System.out.print(db.transact(line));
        line = "select * from t4 where c>0";
        System.out.print(db.transact(line));
    }
}
